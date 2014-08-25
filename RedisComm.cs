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
//#define DEBUG

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

		protected byte [] ReadData (params int [] lookahead)
		{
			int c;
			if (lookahead.Length == 1)
				c = lookahead [0];
			else
				c = bstream.ReadByte ();
			if (c == -1)
				throw new ResponseException("No more data");

			string s = ReadLine ();
			Log ("S", (char)c + s);
			if (c == '-')
				throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
			if (c == '$'){
				if (s == "$-1")
					return null;
				int n;

				if (int.TryParse (s, out n)){
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

			throw new ResponseException ("Unexpected reply: " + (char)c + s);
		}

		// read array of bulk strings
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
			throw new ResponseException("Unknown reply on array request: " + c + s);
		}

		// read array of elements with mixed type (bulk string, integer, nested array)
		protected object [] ReadMixedArray (params int[] lookahead)
		{
			int c;
			if (lookahead.Length == 1)
				c = lookahead [0];
			else
				c = bstream.ReadByte ();
			if (c == -1)
				throw new ResponseException("No more data");

			string s = ReadLine();
			Log("S", (char)c + s);
			if (c == '-')
				throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
			if (c == '*') {
				int count;
				if (int.TryParse (s, out count)) {
					object [] result = new object [count];

					for (int i = 0; i < count; i++)
					{
						int peek = bstream.ReadByte ();
						if (peek == '$')
							result[i] = ReadData (peek);
						else if (peek == ':')
							result[i] = ReadInt (peek);
						else if (peek == '*')
							result[i] = ReadMixedArray (peek);
						else
							throw new ResponseException("Unknown array element: " + c + s);
					}
					return result;
				}
			}
			throw new ResponseException("Unknown reply on array request: " + c + s);
		}

		byte [] end_line = new byte [] { (byte) '\r', (byte) '\n' };

		protected bool SendDataCommand (byte [] data, string cmd, params object [] args)
		{
			MemoryStream ms = new MemoryStream ();
			string resp = "*" + (1 + args.Length + 1) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}
			resp +=	"$" + data.Length + "\r\n";
			byte [] r = Encoding.UTF8.GetBytes (resp);
			ms.Write (r, 0, r.Length);
			ms.Write (data, 0, data.Length);
			ms.Write (end_line, 0, end_line.Length);

			Log ("C", resp);
			return SendRaw (ms.ToArray ());
		}

		protected void SendTuplesCommand (object [] ids, byte [][] values, string cmd, params object [] args)
		{
			if (ids.Length != values.Length)
				throw new ArgumentException ("id's and values must have the same size");

			MemoryStream ms = new MemoryStream ();
			string resp = "*" + (1 + 2 * ids.Length + args.Length) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}
			byte [] r = Encoding.UTF8.GetBytes (resp);
			ms.Write (r, 0, r.Length);

			for (int i = 0; i < ids.Length; i++) {
				string idStr = string.Format (CultureInfo.InvariantCulture, "{0}", ids[i]);
				byte [] id = Encoding.UTF8.GetBytes (idStr);
				byte [] val = values[i];
				byte [] idLength = Encoding.UTF8.GetBytes ("$" + id.Length + "\r\n");
				byte [] valLength = Encoding.UTF8.GetBytes ("$" + val.Length + "\r\n");
				ms.Write (idLength, 0, idLength.Length);
				ms.Write (id, 0, id.Length);
				ms.Write (end_line, 0, end_line.Length);
				ms.Write (valLength, 0, valLength.Length);
				ms.Write (val, 0, val.Length);
				ms.Write (end_line, 0, end_line.Length);
			}

			Log ("C", resp);
			SendRaw (ms.ToArray ());
		}

		private bool SendRaw (byte [] r)
		{
			if (socket == null)
				Connect ();
			if (socket == null)
				return false;

			try {
				socket.Send (r);
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
			string resp = "*" + (1 + args.Length) + "\r\n";
			resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
			foreach (object arg in args) {
				string argStr = string.Format (CultureInfo.InvariantCulture, "{0}", arg);
				int argStrLength = Encoding.UTF8.GetByteCount(argStr);
				resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
			}

			Log ("C", resp);
			return SendRaw (Encoding.UTF8.GetBytes (resp));
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

		protected int ReadInt (params int [] lookahead)
		{
			int c;
			if (lookahead.Length == 1)
				c = lookahead [0];
			else
				c = bstream.ReadByte ();
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

			return ReadInt();
		}

		protected int SendExpectInt (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ReadInt();
		}

		protected string ReadString ()
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

			throw new ResponseException ("Unknown reply on string request: " + c + s);
		}

		protected string SendExpectString (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ReadString();
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

		protected string [] SendExpectStringArray (string cmd, params object [] args)
		{
			byte [][] reply = SendExpectDataArray (cmd, args);
			return ToStringArray (reply);
		}

		protected byte [] SendExpectData (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception ("Unable to connect");

			return ReadData ();
		}

		protected byte[][] SendExpectDataArray (string cmd, params object [] args)
		{
			if (!SendCommand (cmd, args))
				throw new Exception("Unable to connect");
			return ReadDataArray();
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

		#region Conversion between text strings and bulk strings
		public static byte [] ToData (string text)
		{
			return Encoding.UTF8.GetBytes (text);
		}

		public static string ToString (byte [] data)
		{
			return Encoding.UTF8.GetString (data);
		}

		public static string [] ToStringArray (byte [][] dataArray)
		{
			string [] result = new string [dataArray.Length];
			for (int i = 0; i < result.Length; i++)
				result[i] = Encoding.UTF8.GetString (dataArray[i]);
			return result;
		}
		#endregion
	}
}
