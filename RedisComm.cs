//
// RedisComm.cs: ECMA CLI Binding to the Redis key-value storage system
//
//   Communication and protocol handling
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of redis: new BSD license.
//
#define DEBUG

using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Globalization;
using System.Diagnostics;

namespace RedisSharp {
	public class RedisComm : IDisposable {
		Socket socket;
		BufferedStream bstream;

		public enum KeyType {
			None, String, List, Set
		}

		public class ResponseException : Exception {
			public ResponseException (string code) : base ("Response error")
			{
				Code = code;
			}

			public string Code { get; private set; }
		}

		public RedisComm (string host, int port)
		{
			if (host == null)
				throw new ArgumentNullException ("host");

			Host = host;
			Port = port;
			SendTimeout = -1;
		}

		public RedisComm (string host) : this (host, 6379)
		{
		}

		public RedisComm () : this ("localhost", 6379)
		{
		}

		public string Host { get; private set; }
		public int Port { get; private set; }
		public int RetryTimeout { get; set; }
		public int RetryCount { get; set; }
		public int SendTimeout { get; set; }
		public string Password { get; set; }

		[Conditional ("DEBUG")]
		protected void Log (string id, string message)
		{
			Console.WriteLine(id + ": " + message.Trim().Replace("\r\n", " "));
		}

		void Connect ()
		{
			socket = new Socket (AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
			socket.NoDelay = true;
			socket.SendTimeout = SendTimeout;
			socket.Connect (Host, Port);
			if (!socket.Connected){
				socket.Close ();
				socket = null;
				return;
			}
			bstream = new BufferedStream (new NetworkStream (socket), 16*1024);

			if (Password != null)
				SendExpectSuccess ("AUTH", Password);
		}

		protected string ReadLine ()
		{
			StringBuilder sb = new StringBuilder ();
			int c;
			
			while ((c = bstream.ReadByte ()) != -1){
				if (c == '\r')
					continue;
				if (c == '\n')
					break;
				sb.Append ((char) c);
			}
			return sb.ToString ();
		}

		protected byte [] ReadData ()
		{
			string s = ReadLine ();
			Log ("S", s);
			if (s.Length == 0)
				throw new ResponseException ("Zero length respose");

			char c = s [0];
			if (c == '-')
				throw new ResponseException (s.StartsWith ("-ERR ") ? s.Substring (5) : s.Substring (1));

			if (c == '$'){
				if (s == "$-1")
					return null;
				int n;

				if (int.TryParse (s.Substring (1), out n)){
					byte [] retbuf = new byte [n];

					int bytesRead = 0;
					do {
						int read = bstream.Read (retbuf, bytesRead, n - bytesRead);
						if (read < 1)
							throw new ResponseException("Invalid termination mid stream");
						bytesRead += read;
					}
					while (bytesRead < n);
					if (bstream.ReadByte () != '\r' || bstream.ReadByte () != '\n')
						throw new ResponseException ("Invalid termination");
					return retbuf;
				}
				throw new ResponseException ("Invalid length");
			}
			else if (c == ':') {
				// return unparsed integer
				return Encoding.UTF8.GetBytes(s.Substring(1));
			}

			throw new ResponseException ("Unexpected reply: " + s);
		}

		protected byte[][] ReadDataArray ()
		{
			int c = bstream.ReadByte();
			if (c == -1)
				throw new ResponseException("No more data");

			string s = ReadLine();
			Log("S", (char)c + s);
			if (c == '-')
				throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
			if (c == '*') {
				int count;
				if (int.TryParse (s, out count)) {
					byte [][] result = new byte [count][];

					for (int i = 0; i < count; i++)
						result[i] = ReadData();

					return result;
				}
			}
			throw new ResponseException("Unknown reply on multi-request: " + c + s);
		}

		byte [] end_data = new byte [] { (byte) '\r', (byte) '\n' };

		protected bool SendDataCommand (byte [] data, string cmd, params object [] args)
		{
			string resp = "*" + (1 + args.Length + 1) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}
			resp +=	"$" + data.Length + "\r\n";

			return SendDataRESP (data, resp);
		}

		protected void SendTuplesCommand (object [] keys, byte [][] values, string cmd, params object [] args)
		{
			if (keys.Length != values.Length)
				throw new ArgumentException ("keys and values must have the same size");

			byte [] nl = Encoding.UTF8.GetBytes ("\r\n");
			MemoryStream ms = new MemoryStream ();

			for (int i = 0; i < keys.Length; i++) {
				string keyStr = string.Format (CultureInfo.InvariantCulture, "{0}", keys[i]);
				byte [] key = Encoding.UTF8.GetBytes (keyStr);
				byte [] val = values[i];
				byte [] kLength = Encoding.UTF8.GetBytes ("$" + key.Length + "\r\n");
				byte [] k = Encoding.UTF8.GetBytes (keys[i] + "\r\n");
				byte [] vLength = Encoding.UTF8.GetBytes ("$" + val.Length + "\r\n");
				ms.Write (kLength, 0, kLength.Length);
				ms.Write (k, 0, k.Length);
				ms.Write (vLength, 0, vLength.Length);
				ms.Write (val, 0, val.Length);
				ms.Write (nl, 0, nl.Length);
			}

			string resp = "*" + (1 + keys.Length * 2 + args.Length) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}

			SendDataRESP (ms.ToArray (), resp);
		}

		private bool SendDataRESP (byte [] data, string resp)
		{
			if (socket == null)
				Connect ();
			if (socket == null)
				return false;

			byte [] r = Encoding.UTF8.GetBytes (resp);
			try {
				Log ("C", resp);
				socket.Send (r);
				if (data != null){
					socket.Send (data);
					socket.Send (end_data);
				}
			} catch (SocketException){
				// timeout;
				socket.Close ();
				socket = null;

				return false;
			}
			return true;
		}

		protected bool SendCommand (string cmd, params object [] args)
		{
			if (socket == null)
				Connect ();
			if (socket == null)
				return false;

			string resp = "*" + (1 + args.Length) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}

			byte [] r = Encoding.UTF8.GetBytes (resp);
			try {
				Log ("C", resp);
				socket.Send (r);
			} catch (SocketException){
				// timeout;
				socket.Close ();
				socket = null;

				return false;
			}
			return true;
		}

		protected void ExpectSuccess ()
		{
			int c = bstream.ReadByte ();
			if (c == -1)
				throw new ResponseException ("No more data");

			string s = ReadLine ();
			Log ("S", (char)c + s);
			if (c == '-')
				throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
		}

		protected void SendExpectSuccess (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			ExpectSuccess ();
		}

		protected int ExpectInt ()
		{
			int c = bstream.ReadByte ();
			if (c == -1)
				throw new ResponseException ("No more data");

			string s = ReadLine ();
			Log ("S", (char)c + s);
			if (c == '-')
				throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
			if (c == ':'){
				int i;
				if (int.TryParse (s, out i))
					return i;
			}
			throw new ResponseException ("Unknown reply on integer request: " + c + s);
		}

		protected int SendDataExpectInt (byte[] data, string cmd, params object [] args)
		{
			if (!SendDataCommand (data, cmd, args))
				throw new Exception ("Unable to connect");

			return ExpectInt();
		}

		protected int SendExpectInt (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ExpectInt();
		}

		protected string ExpectString ()
		{
			int c = bstream.ReadByte ();
			if (c == -1)
				throw new ResponseException ("No more data");

			string s = ReadLine ();
			Log ("S", (char)c + s);
			if (c == '-')
				throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
			if (c == '+')
				return s;

			throw new ResponseException ("Unknown reply on integer request: " + c + s);
		}

		protected string SendExpectString (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ExpectString();
		}

		//
		// This one does not throw errors
		//
		protected string SendGetString (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ReadLine ();
		}

		protected byte [] SendExpectData (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ReadData ();
		}

		public void Shutdown ()
		{
			SendCommand ("SHUTDOWN");
			try {
				// the server may return an error
				string s = ReadLine ();
				Log ("S", s);
				if (s.Length == 0)
					throw new ResponseException ("Zero length respose");
				throw new ResponseException (s.StartsWith ("-ERR ") ? s.Substring (5) : s.Substring (1));
			} catch (IOException) {
				// this is the expected good result
				socket.Close ();
				socket = null;
			}
		}

		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		~RedisComm ()
		{
			Dispose (false);
		}

		protected virtual void Dispose (bool disposing)
		{
			if (disposing){
				socket.Close ();
				socket = null;
			}
		}
	}
}
