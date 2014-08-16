//
// redis-sharp.cs: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
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

public class Redis : IDisposable {
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
	
	public Redis (string host, int port)
	{
		if (host == null)
			throw new ArgumentNullException ("host");
		
		Host = host;
		Port = port;
		SendTimeout = -1;
	}
	
	public Redis (string host) : this (host, 6379)
	{
	}
	
	public Redis () : this ("localhost", 6379)
	{
	}

	public string Host { get; private set; }
	public int Port { get; private set; }
	public int RetryTimeout { get; set; }
	public int RetryCount { get; set; }
	public int SendTimeout { get; set; }
	public string Password { get; set; }
	
	int db;
	public int Db {
		get {
			return db;
		}

		set {
			db = value;
			SendExpectSuccess (ToRESP("SELECT", db));
		}
	}

	public string this [string key] {
		get { return GetString (key); }
		set { Set (key, value); }
	}

	public void Set (string key, string value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");
		
		Set (key, Encoding.UTF8.GetBytes (value));
	}
	
	public void Set (string key, byte [] value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");

		if (value.Length > 1073741824)
			throw new ArgumentException ("value exceeds 1G", "value");

		if (!SendDataCommand (value, ToDataRESP("SET", key, value.Length)))
			throw new Exception ("Unable to connect");
		ExpectSuccess ();
	}

	public bool SetNX (string key, string value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");
		
		return SetNX (key, Encoding.UTF8.GetBytes (value));
	}
	
	public bool SetNX (string key, byte [] value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");

		if (value.Length > 1073741824)
			throw new ArgumentException ("value exceeds 1G", "value");

		return SendDataExpectInt (value, ToDataRESP("SETNX", key, value.Length)) > 0 ? true : false;
	}

	public void Set (IDictionary<string,string> dict)
	{
	  Set(dict.ToDictionary(k => k.Key, v => Encoding.UTF8.GetBytes(v.Value)));
	}

	public void Set (IDictionary<string,byte []> dict)
	{
		if (dict == null)
			throw new ArgumentNullException ("dict");

		var nl = Encoding.UTF8.GetBytes ("\r\n");

		var ms = new MemoryStream ();
		foreach (var key in dict.Keys){
			var val = dict [key];

			var kLength = Encoding.UTF8.GetBytes ("$" + key.Length + "\r\n");
			var k = Encoding.UTF8.GetBytes (key + "\r\n");
			var vLength = Encoding.UTF8.GetBytes ("$" + val.Length + "\r\n");
			ms.Write (kLength, 0, kLength.Length);
			ms.Write (k, 0, k.Length);
			ms.Write (vLength, 0, vLength.Length);
			ms.Write (val, 0, val.Length);
			ms.Write (nl, 0, nl.Length);
		}
		
		SendDataCommand (ms.ToArray (), "*" + (dict.Count * 2 + 1) + "\r\n$4\r\nMSET\r\n");
		ExpectSuccess ();
	}

	public byte [] Get (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectData (null, ToRESP("GET", key));
	}

	public string GetString (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return Encoding.UTF8.GetString (Get (key));
	}

	public byte[][] Sort (SortOptions options)
	{
		return SendDataCommandExpectMultiBulkReply(null, options.ToCommand() + "\r\n");
	}
	
	public byte [] GetSet (string key, byte [] value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");
		
		if (value.Length > 1073741824)
			throw new ArgumentException ("value exceeds 1G", "value");

		if (!SendDataCommand (value, ToDataRESP("GETSET", key, value.Length)))
			throw new Exception ("Unable to connect");

		return ReadData ();
	}

	public string GetSet (string key, string value)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		if (value == null)
			throw new ArgumentNullException ("value");
		return Encoding.UTF8.GetString (GetSet (key, Encoding.UTF8.GetBytes (value)));
	}
	
	string ReadLine ()
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
			SendExpectSuccess (ToRESP("AUTH", Password));
	}

	byte [] end_data = new byte [] { (byte) '\r', (byte) '\n' };
	
	bool SendDataCommand (byte [] data, string cmd, params object [] args)
	{
		if (socket == null)
			Connect ();
		if (socket == null)
			return false;

		string s = args.Length > 0 ? String.Format (cmd, args) : cmd;
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

	bool SendCommand (string cmd, params object [] args)
	{
		if (socket == null)
			Connect ();
		if (socket == null)
			return false;

		string s = args != null && args.Length > 0 ? String.Format (cmd, args) : cmd;
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
	void Log (string fmt, params object [] args)
	{
		Console.WriteLine("{0}", String.Format(fmt.Replace("\r\n", " "), args).Trim());
	}

	void ExpectSuccess ()
	{
		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		string s = ReadLine ();
		Log ((char)c + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
	}
	
	void SendExpectSuccess (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		ExpectSuccess ();
	}	

	int SendDataExpectInt (byte[] data, string cmd, params object [] args)
	{
		if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		string s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
		if (c == ':'){
			int i;
			if (int.TryParse (s, out i))
				return i;
		}
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	int SendExpectInt (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		string s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
		if (c == ':'){
			int i;
			if (int.TryParse (s, out i))
				return i;
		}
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	string SendExpectString (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		int c = bstream.ReadByte ();
		if (c == -1)
			throw new ResponseException ("No more data");

		string s = ReadLine ();
		Log ("R: " + s);
		if (c == '-')
			throw new ResponseException (s.StartsWith ("ERR ") ? s.Substring (4) : s);
		if (c == '+')
			return s;
		
		throw new ResponseException ("Unknown reply on integer request: " + c + s);
	}	

	//
	// This one does not throw errors
	//
	string SendGetString (string cmd, params object [] args)
	{
		if (!SendCommand (cmd, args))
			throw new Exception ("Unable to connect");

		return ReadLine ();
	}	
	
	byte [] SendExpectData (byte[] data, string cmd, params object [] args)
	{
		if (!SendDataCommand (data, cmd, args))
			throw new Exception ("Unable to connect");

		return ReadData ();
	}

	byte [] ReadData ()
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

		/* don't treat arrays here because only one element works -- use MultiBulkReply!
		//returns the number of matches
		if (c == '*') {
			int n;
			if (Int32.TryParse(r.Substring(1), out n)) 
				return n <= 0 ? new byte [0] : ReadData();
			
			throw new ResponseException ("Unexpected length parameter" + r);
		}
		*/

		throw new ResponseException ("Unexpected reply: " + r);
	}	

	public bool ContainsKey (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("EXISTS", key)) == 1;
	}

	public bool Remove (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("DEL", key)) == 1;
	}

	public int Remove (params string [] args)
	{
		if (args == null)
			throw new ArgumentNullException ("args");
		return SendExpectInt (ToRESP("DEL", args));
	}

	public int Increment (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("INCR", key));
	}

	public int Increment (string key, int count)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("INCRBY", key, count));
	}

	public int Decrement (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("DECR", key));
	}

	public int Decrement (string key, int count)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("DECRBY", key, count));
	}

	public KeyType TypeOf (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		switch (SendExpectString (ToRESP("TYPE", key))) {
		case "none":
			return KeyType.None;
		case "string":
			return KeyType.String;
		case "set":
			return KeyType.Set;
		case "list":
			return KeyType.List;
		}
		throw new ResponseException ("Invalid value");
	}

	public string RandomKey ()
	{
		return SendExpectString (ToRESP("RANDOMKEY"));
	}

	public bool Rename (string oldKeyname, string newKeyname)
	{
		if (oldKeyname == null)
			throw new ArgumentNullException ("oldKeyname");
		if (newKeyname == null)
			throw new ArgumentNullException ("newKeyname");
		return SendGetString (ToRESP("RENAME", oldKeyname, newKeyname)) [0] == '+';
	}

	public bool Expire (string key, int seconds)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("EXPIRE", key, seconds)) == 1;
	}

	public bool ExpireAt (string key, int time)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("EXPIREAT", key, time)) == 1;
	}

	public int TimeToLive (string key)
	{
		if (key == null)
			throw new ArgumentNullException ("key");
		return SendExpectInt (ToRESP("TTL", key));
	}
	
	public int DbSize {
		get {
			return SendExpectInt (ToRESP("DBSIZE"));
		}
	}

	public void Save ()
	{
		SendExpectSuccess (ToRESP("SAVE"));
	}

	public void BackgroundSave ()
	{
		SendExpectSuccess (ToRESP("BGSAVE"));
	}

	public void Shutdown ()
	{
		SendExpectSuccess (ToRESP("SHUTDOWN"));
	}

	public void FlushAll ()
	{
		SendExpectSuccess (ToRESP("FLUSHALL"));
	}
	
	public void FlushDb ()
	{
		SendExpectSuccess (ToRESP("FLUSHDB"));
	}

	const long UnixEpoch = 621355968000000000L;
	
	public DateTime LastSave {
		get {
			int t = SendExpectInt (ToRESP("LASTSAVE"));
			
			return new DateTime (UnixEpoch) + TimeSpan.FromSeconds (t);
		}
	}
	
	public Dictionary<string,string> GetInfo ()
	{
		byte [] r = SendExpectData (null, ToRESP("INFO"));
		var dict = new Dictionary<string,string>();
		
		foreach (var line in Encoding.UTF8.GetString (r).Split ('\n')){
			int p = line.IndexOf (':');
			if (p == -1)
				continue;
			dict.Add (line.Substring (0, p), line.Substring (p+1));
		}
		return dict;
	}

	public string [] Keys {
		get {
			return GetKeys("*");
		}
	}

	public string [] GetKeys (string pattern)
	{
		if (pattern == null)
			throw new ArgumentNullException ("key");
		byte [][] reply = SendDataCommandExpectMultiBulkReply (null, ToRESP("KEYS", pattern));
		string [] keys = new string [reply.Length];
		for (int i = 0; i < reply.Length; i++)
			keys[i] = Encoding.UTF8.GetString (reply[i]);
		return keys;
	}

	public byte [][] MGet (params string [] keys)
	{
		if (keys == null)
			throw new ArgumentNullException ("key1");
		if (keys.Length == 0)
			throw new ArgumentException ("keys");
		
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("MGET", keys));
	}


	public byte[][] SendDataCommandExpectMultiBulkReply(byte[] data, string command, params object[] args)
	{
		if (!SendDataCommand(data, command, args))
			throw new Exception("Unable to connect");
		int c = bstream.ReadByte();
		if (c == -1)
			throw new ResponseException("No more data");
		
		string s = ReadLine();
		Log("R: " + s);
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
	
	#region List commands
	public byte[][] ListRange(string key, int start, int end)
	{
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("LRANGE", key, start, end));
	}

	public void LeftPush(string key, string value)
	{
		LeftPush(key, Encoding.UTF8.GetBytes (value));
	}

	public void LeftPush(string key, byte [] value)
	{
		SendDataCommand (value, ToDataRESP("LPUSH", key, value.Length));
		ExpectSuccess();
	}

	public void RightPush(string key, string value)
	{
		RightPush(key, Encoding.UTF8.GetBytes (value));
	}

	public void RightPush(string key, byte [] value)
	{
		SendDataCommand (value, ToDataRESP("RPUSH", key, value.Length));
		ExpectSuccess();
	}

	public int ListLength (string key)
	{
		return SendExpectInt (ToRESP("LLEN", key));
	}

	public byte[] ListIndex (string key, int index)
	{
		SendCommand (ToRESP("LINDEX", key, index));
		return ReadData ();
	}

	public byte[] LeftPop(string key)
	{
		SendCommand (ToRESP("LPOP", key));
		return ReadData ();
	}

	public byte[] RightPop(string key)
	{
		SendCommand (ToRESP("RPOP", key));
		return ReadData ();
	}
	#endregion

	#region Set commands
	public bool AddToSet (string key, byte[] member)
	{
		return SendDataExpectInt(member, ToDataRESP("SADD", key, member.Length)) > 0;
	}

	public bool AddToSet (string key, string member)
	{
		return AddToSet (key, Encoding.UTF8.GetBytes(member));
	}
	
	public int CardinalityOfSet (string key)
	{
		return SendDataExpectInt (null, ToRESP("SCARD", key));
	}

	public bool IsMemberOfSet (string key, byte[] member)
	{
		return SendDataExpectInt (member, ToDataRESP("SISMEMBER", key, member.Length)) > 0;
	}

	public bool IsMemberOfSet(string key, string member)
	{
		return IsMemberOfSet(key, Encoding.UTF8.GetBytes(member));
	}
	
	public byte[][] GetMembersOfSet (string key)
	{
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("SMEMBERS", key));
	}
	
	public byte[] GetRandomMemberOfSet (string key)
	{
		return SendExpectData (null, ToRESP("SRANDMEMBER", key));
	}
	
	public byte[] PopRandomMemberOfSet (string key)
	{
		return SendExpectData (null, ToRESP("SPOP", key));
	}

	public bool RemoveFromSet (string key, byte[] member)
	{
		return SendDataExpectInt (member, ToDataRESP("SREM", key, member.Length)) > 0;
	}

	public bool RemoveFromSet (string key, string member)
	{
		return RemoveFromSet (key, Encoding.UTF8.GetBytes(member));
	}
		
	public byte[][] GetUnionOfSets (params string[] keys)
	{
		if (keys == null)
			throw new ArgumentNullException();
		
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("SUNION", keys));
		
	}
	
	void StoreSetCommands (string cmd, string destKey, params string[] keys)
	{
		if (String.IsNullOrEmpty(cmd))
			throw new ArgumentNullException ("cmd");
		
		if (String.IsNullOrEmpty(destKey))
			throw new ArgumentNullException ("destKey");
		
		if (keys == null)
			throw new ArgumentNullException ("keys");
		
		SendExpectSuccess (ToRESP2(cmd, destKey, keys));
	}
	
	public void StoreUnionOfSets (string destKey, params string[] keys)
	{
		StoreSetCommands ("SUNIONSTORE", destKey, keys);
	}
	
	public byte[][] GetIntersectionOfSets (params string[] keys)
	{
		if (keys == null)
			throw new ArgumentNullException();
		
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("SINTER", keys));
	}
	
	public void StoreIntersectionOfSets (string destKey, params string[] keys)
	{
		StoreSetCommands ("SINTERSTORE", destKey, keys);
	}
	
	public byte[][] GetDifferenceOfSets (params string[] keys)
	{
		if (keys == null)
			throw new ArgumentNullException();
		
		return SendDataCommandExpectMultiBulkReply (null, ToRESP("SDIFF", keys));
	}
	
	public void StoreDifferenceOfSets (string destKey, params string[] keys)
	{
		StoreSetCommands ("SDIFFSTORE", destKey, keys);
	}
	
	public bool MoveMemberToSet (string srcKey, string destKey, byte[] member)
	{
		return SendDataExpectInt (member, ToDataRESP2("SMOVE", srcKey, destKey, member.Length)) > 0;
	}
	#endregion

	string ToRESP (string command, params object [] args)
	{
		string resp = "*" + (1 + args.Length).ToString () + "\r\n";
		resp += "$" + command.Length + "\r\n" + command + "\r\n";
		foreach (object arg in args) {
			string argStr = arg.ToString ();
			resp += "$" + argStr.Length + "\r\n" + argStr + "\r\n";
		}
		return resp;
	}

	string ToRESP2 (string command, string key, params object [] args)
	{
		string resp = "*" + (2 + args.Length).ToString () + "\r\n";
		resp += "$" + command.Length + "\r\n" + command + "\r\n";
		resp += "$" + key.Length + "\r\n" + key + "\r\n";
		foreach (object arg in args) {
			string argStr = arg.ToString ();
			resp += "$" + argStr.Length + "\r\n" + argStr + "\r\n";
		}
		return resp;
	}

	string ToDataRESP (string command, string key, int dataLength)
	{
		return "*3\r\n$" + command.Length + "\r\n" + command + "\r\n"
			+ "$" + key.Length + "\r\n" + key + "\r\n"
			+ "$" + dataLength + "\r\n";
	}

	string ToDataRESP2 (string command, string key1, string key2, int dataLength)
	{
		return "*4\r\n$" + command.Length + "\r\n" + command + "\r\n"
			+ "$" + key1.Length + "\r\n" + key1 + "\r\n"
			+ "$" + key2.Length + "\r\n" + key2 + "\r\n"
			+ "$" + dataLength + "\r\n";
	}

	public void Dispose ()
	{
		Dispose (true);
		GC.SuppressFinalize (this);
	}

	~Redis ()
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
		string command = "SORT " + this.Key;
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

