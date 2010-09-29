//
// RedisBase.cs: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//   Jonathan R. Steele (jrsteele@gmail.com)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
#define DEBUG

using System;
using System.IO;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Diagnostics;
using System.Linq;

public abstract class RedisBase : IDisposable {
	
	protected Socket socket;
	protected BufferedStream bstream;
	
	public string Host { get; private set; }
	public int Port { get; private set; }
	public int RetryTimeout { get; set; }
	public int RetryCount { get; set; }
	public int SendTimeout { get; set; }
	public string Password { get; set; }
	
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
	
		
	protected RedisBase(string host, int port)
	{
		if (host == null)
			throw new ArgumentNullException ("host");
		
		Host = host;
		Port = port;
		SendTimeout = -1;
	}
	
	protected int db;
	public int Db {
		get {
			return db;
		}

		set {
			db = value;
			SendExpectSuccess ("SELECT {0}\r\n", db);
		}
	}
	
	#region Public Methods
	public Dictionary<string,string> GetInfo ()
	{
		byte [] r = SendExpectData (null, "INFO\r\n");
		var dict = new Dictionary<string,string>();
		
		foreach (var line in Encoding.UTF8.GetString (r).Split ('\n')){
			int p = line.IndexOf (':');
			if (p == -1)
				continue;
			dict.Add (line.Substring (0, p), line.Substring (p+1));
		}
		return dict;
	}
	#endregion
	
	#region Helper Methods
	protected string ReadLine ()
	{
		var sb = new StringBuilder ();
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
	
	protected void Connect ()
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
			SendExpectSuccess ("AUTH {0}\r\n", Password);
	}

	protected byte [] end_data = new byte [] { (byte) '\r', (byte) '\n' };
	
	protected bool SendDataCommand (byte [] data, string cmd, params object [] args)
	{
		if (socket == null)
			Connect ();
		if (socket == null)
			return false;

		var s = args.Length > 0 ? String.Format (cmd, args) : cmd;
		byte [] r = Encoding.UTF8.GetBytes (s);
		try {
			Log ("S: " + String.Format (cmd, args));
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

		var s = args != null && args.Length > 0 ? String.Format (cmd, args) : cmd;
		byte [] r = Encoding.UTF8.GetBytes (s);
		try {
			Log ("S: " + String.Format (cmd, args));
			socket.Send (r);
		} catch (SocketException){
			// timeout;
			socket.Close ();
			socket = null;

			return false;
		}
		return true;
	}
	
	[Conditional ("DEBUG")]
	protected void Log (string fmt, params object [] args)
	{
		Console.WriteLine ("{0}", String.Format (fmt, args).Trim ());
	}

	protected void ExpectSuccess ()
	{
		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ((char)c + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
	}
	
	protected void SendExpectSuccess (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		ExpectSuccess ();
	}	

	protected int SendDataExpectInt (byte[] data, string cmd, params object [] args)
	{
		if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
		if (c == ':'){
			int i;
			if (int.TryParse (s, out i))
				return i;
		}
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	protected int SendExpectInt (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
		if (c == ':'){
			int i;
			if (int.TryParse (s, out i))
				return i;
		}
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	protected string SendExpectString (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		var s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR") ? s.Substring (4) : s);
		if (c == '+')
			return s;
		
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
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
	
	protected byte [] SendExpectData (byte[] data, string cmd, params object [] args)
	{
		if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

		return ReadData ();
	}

	protected byte [] ReadData ()
	{
		string r = ReadLine ();
		Log ("R: {0}", r);
		if (r.Length == 0)
			throw new ResponseException ("Zero length respose");
		
		char c = r [0];
		if (c == '-')
			throw new ResponseException (r.StartsWith ("-ERR") ? r.Substring (5) : r.Substring (1));

		if (c == '$'){
			if (r == "$-1")
				return null;
			int n;
			
			if (Int32.TryParse (r.Substring (1), out n)){
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

		//returns the number of matches
		if (c == '*') {
			int n;
			if (Int32.TryParse(r.Substring(1), out n)) 
				return n <= 0 ? new byte [0] : ReadData();
			
			throw new ResponseException ("Unexpected length parameter" + r);
		}
		
		/* JS (09/27/2010):
		 * 	This is needed for handling messages that come in via (p)subscribe commands.
		 */
		if (c == ':') {
				int n;
				if (Int32.TryParse(r.Substring(1), out n))
					return n <= 0 ? new byte[0] : ReadData();
				
			}
		
		throw new ResponseException ("Unexpected reply: " + r);
	}	
	
	/// <summary>
	/// Require a minimum version. 
	/// </summary>
	protected void RequireMinimumVersion(string version)
	{
		var info = GetInfo();
		string ver = info["redis_version"];
		
		if (ver.CompareTo(version) < 0)
			throw new Exception(String.Format("Expecting Redis version {0}, but got {1}", version, ver));
	}
	
	#endregion
	
	#region Cleanup methods
	public void Dispose ()
	{
		Dispose (true);
		GC.SuppressFinalize (this);
	}

	~RedisBase ()
	{
		Dispose (false);
	}
	
	protected virtual void Dispose (bool disposing)
	{
		if (disposing){
			SendCommand ("QUIT\r\n");
			socket.Close ();
			socket = null;
		}
	}
	#endregion
	
}



public class SortOptions {
	public string Key { get; set; }
	public bool Descending { get; set; }
	public bool Lexographically { get; set; }
	public Int32 LowerLimit { get; set; }
	public Int32 UpperLimit { get; set; }
	public string By { get; set; }
	public string StoreInKey { get; set; }
	public string Get { get; set; }
	
	public string ToCommand()
	{
		var command = "SORT " + this.Key;
		if (LowerLimit != 0 || UpperLimit != 0)
			command += " LIMIT " + LowerLimit + " " + UpperLimit;
		if (Lexographically)
			command += " ALPHA";
		if (!string.IsNullOrEmpty (By))
			command += " BY " + By;
		if (!string.IsNullOrEmpty (Get))
			command += " GET " + Get;
		if (!string.IsNullOrEmpty (StoreInKey))
			command += " STORE " + StoreInKey;
		return command;
	}
}


