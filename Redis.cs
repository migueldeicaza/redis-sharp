//
// Redis.cs: ECMA CLI Binding to the Redis key-value storage system
//
//   Synchronous client
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of redis: new BSD license.
//

using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace RedisSharp {
	public class Redis : RedisComm {

		public Redis (string host, int port) : base (host, port)
		{
		}

		public Redis (string host) : this (host, 6379)
		{
		}

		public Redis () : this ("localhost", 6379)
		{
		}

		#region Connection commands
		int db;
		public int Db {
			get {
				return db;
			}

			set {
				db = value;
				SendExpectSuccess ("SELECT", db);
			}
		}
		#endregion

		#region String commands
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

			if (!SendDataCommand (value, "SET", key))
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

			return SendDataExpectInt (value, "SETNX", key) > 0 ? true : false;
		}

		public void Set (IDictionary<string,string> dict)
		{
			if (dict == null)
				throw new ArgumentNullException ("dict");

			Set (dict.ToDictionary(k => k.Key, v => Encoding.UTF8.GetBytes(v.Value)));
		}

		public void Set (IDictionary<string,byte []> dict)
		{
			if (dict == null)
				throw new ArgumentNullException ("dict");

			MSet (dict.Keys.ToArray (), dict.Values.ToArray ());
		}

		public void MSet (string [] keys, byte [][] values)
		{
			SendTuplesCommand (keys, values, "MSET");
			ExpectSuccess ();
		}

		public byte [] Get (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectData ("GET", key);
		}

		public string GetString (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return Encoding.UTF8.GetString (Get (key));
		}

		public byte [] GetSet (string key, byte [] value)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			if (value == null)
				throw new ArgumentNullException ("value");

			if (value.Length > 1073741824)
				throw new ArgumentException ("value exceeds 1G", "value");

			if (!SendDataCommand (value, "GETSET", key))
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

		public byte [][] MGet (params string [] keys)
		{
			if (keys == null)
				throw new ArgumentNullException ("keys");
			if (keys.Length == 0)
				throw new ArgumentException ("keys");

			return SendExpectDataArray ("MGET", keys);
		}

		public int Increment (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("INCR", key);
		}

		public int Increment (string key, int count)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("INCRBY", key, count);
		}

		public int Decrement (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("DECR", key);
		}

		public int Decrement (string key, int count)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("DECRBY", key, count);
		}
		#endregion

		#region Key commands
		public string [] Keys {
			get {
				return GetKeys("*");
			}
		}

		public string [] GetKeys (string pattern)
		{
			if (pattern == null)
				throw new ArgumentNullException ("pattern");

			return SendExpectStringArray ("KEYS", pattern);
		}

		public byte [][] Sort (SortOptions options)
		{
			if (options.StoreInKey != null) {
				int n = SortStore (options.Key, options.StoreInKey, options.ToArgs ());
				return new byte [n][];
			}
			else {
				return Sort (options.Key, options.ToArgs ());
			}
		}

		public byte [][] Sort (string key, params object [] options)
		{
			if (key == null)
				throw new ArgumentNullException ("key");

			object [] args = new object [1 + options.Length];
			args [0] = key;
			Array.Copy (options, 0, args, 1, options.Length);

			return SendExpectDataArray ("SORT", args);
		}

		public int SortStore (string key, string destination, params object [] options)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			if (destination == null)
				throw new ArgumentNullException ("destination");

			object [] args = new object [3 + options.Length];
			args [0] = key;
			args [1] = "STORE";
			args [2] = destination;
			Array.Copy (options, 0, args, 3, options.Length);

			return SendExpectInt ("SORT", args);
		}

		public bool ContainsKey (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("EXISTS", key) == 1;
		}

		public bool Remove (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("DEL", key) == 1;
		}

		public int Remove (params string [] args)
		{
			if (args == null)
				throw new ArgumentNullException ("args");
			return SendExpectInt ("DEL", args);
		}

		public KeyType TypeOf (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			switch (SendExpectString ("TYPE", key)) {
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
			return SendExpectString ("RANDOMKEY");
		}

		public bool Rename (string oldKeyname, string newKeyname)
		{
			if (oldKeyname == null)
				throw new ArgumentNullException ("oldKeyname");
			if (newKeyname == null)
				throw new ArgumentNullException ("newKeyname");
			return SendGetString ("RENAME", oldKeyname, newKeyname) [0] == '+';
		}

		public bool Expire (string key, int seconds)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("EXPIRE", key, seconds) == 1;
		}

		public bool ExpireAt (string key, int time)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("EXPIREAT", key, time) == 1;
		}

		public int TimeToLive (string key)
		{
			if (key == null)
				throw new ArgumentNullException ("key");
			return SendExpectInt ("TTL", key);
		}
		#endregion

		#region Server commands
		public int DbSize {
			get {
				return SendExpectInt ("DBSIZE");
			}
		}

		public void Save ()
		{
			SendExpectSuccess ("SAVE");
		}

		public void BackgroundSave ()
		{
			SendExpectSuccess ("BGSAVE");
		}

		public void FlushAll ()
		{
			SendExpectSuccess ("FLUSHALL");
		}
		
		public void FlushDb ()
		{
			SendExpectSuccess ("FLUSHDB");
		}

		const long UnixEpoch = 621355968000000000L;

		public DateTime LastSave {
			get {
				int t = SendExpectInt ("LASTSAVE");
				
				return new DateTime (UnixEpoch) + TimeSpan.FromSeconds (t);
			}
		}

		public Dictionary<string,string> GetInfo ()
		{
			byte [] r = SendExpectData ("INFO");
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

		#region List commands
		public byte[][] ListRange(string key, int start, int end)
		{
			return SendExpectDataArray ("LRANGE", key, start, end);
		}

		public void LeftPush(string key, string value)
		{
			LeftPush(key, Encoding.UTF8.GetBytes (value));
		}

		public void LeftPush(string key, byte [] value)
		{
			SendDataCommand (value, "LPUSH", key);
			ExpectSuccess();
		}

		public void RightPush(string key, string value)
		{
			RightPush(key, Encoding.UTF8.GetBytes (value));
		}

		public void RightPush(string key, byte [] value)
		{
			SendDataCommand (value, "RPUSH", key);
			ExpectSuccess();
		}

		public int ListLength (string key)
		{
			return SendExpectInt ("LLEN", key);
		}

		public byte[] ListIndex (string key, int index)
		{
			SendCommand ("LINDEX", key, index);
			return ReadData ();
		}

		public byte[] LeftPop(string key)
		{
			SendCommand ("LPOP", key);
			return ReadData ();
		}

		public byte[] RightPop(string key)
		{
			SendCommand ("RPOP", key);
			return ReadData ();
		}
		#endregion

		#region Set commands
		public bool AddToSet (string key, byte[] member)
		{
			return SendDataExpectInt(member, "SADD", key) > 0;
		}

		public bool AddToSet (string key, string member)
		{
			return AddToSet (key, Encoding.UTF8.GetBytes(member));
		}
		
		public int CardinalityOfSet (string key)
		{
			return SendExpectInt ("SCARD", key);
		}

		public bool IsMemberOfSet (string key, byte[] member)
		{
			return SendDataExpectInt (member, "SISMEMBER", key) > 0;
		}

		public bool IsMemberOfSet(string key, string member)
		{
			return IsMemberOfSet(key, Encoding.UTF8.GetBytes(member));
		}
		
		public byte[][] GetMembersOfSet (string key)
		{
			return SendExpectDataArray ("SMEMBERS", key);
		}
		
		public byte[] GetRandomMemberOfSet (string key)
		{
			return SendExpectData ("SRANDMEMBER", key);
		}

		public byte[] PopRandomMemberOfSet (string key)
		{
			return SendExpectData ("SPOP", key);
		}

		public bool RemoveFromSet (string key, byte[] member)
		{
			return SendDataExpectInt (member, "SREM", key) > 0;
		}

		public bool RemoveFromSet (string key, string member)
		{
			return RemoveFromSet (key, Encoding.UTF8.GetBytes(member));
		}

		public byte[][] GetUnionOfSets (params string[] keys)
		{
			if (keys == null)
				throw new ArgumentNullException();

			return SendExpectDataArray ("SUNION", keys);
		}
		
		void StoreSetCommands (string cmd, params string[] keys)
		{
			if (string.IsNullOrEmpty(cmd))
				throw new ArgumentNullException ("cmd");

			if (keys == null)
				throw new ArgumentNullException ("keys");

			SendExpectSuccess (cmd, keys);
		}

		public void StoreUnionOfSets (params string[] keys)
		{
			StoreSetCommands ("SUNIONSTORE", keys);
		}
		
		public byte[][] GetIntersectionOfSets (params string[] keys)
		{
			if (keys == null)
				throw new ArgumentNullException();

			return SendExpectDataArray ("SINTER", keys);
		}

		public void StoreIntersectionOfSets (params string[] keys)
		{
			StoreSetCommands ("SINTERSTORE", keys);
		}

		public byte[][] GetDifferenceOfSets (params string[] keys)
		{
			if (keys == null)
				throw new ArgumentNullException();

			return SendExpectDataArray ("SDIFF", keys);
		}

		public void StoreDifferenceOfSets (params string[] keys)
		{
			StoreSetCommands ("SDIFFSTORE", keys);
		}

		public bool MoveMemberToSet (string srcKey, string destKey, byte[] member)
		{
			return SendDataExpectInt (member, "SMOVE", srcKey, destKey) > 0;
		}
		#endregion

		#region Pub commands
		public int Publish (string channel, string message)
		{
			return Publish (channel, Encoding.UTF8.GetBytes (message));
		}

		public int Publish (string channel, byte [] message)
		{
			if (channel == null)
				throw new ArgumentNullException ("channel");
			if (message == null)
				throw new ArgumentNullException ("message");

			return SendDataExpectInt (message, "PUBLISH", channel);
		}
		#endregion

		protected override void Dispose (bool disposing)
		{
			if (disposing){
				SendCommand ("QUIT");
				ExpectSuccess ();
			}
			base.Dispose (disposing);
		}
	}

	public class SortOptions {
		public string Key { get; set; }
		public bool Descending { get; set; }
		public bool Lexographically { get; set; }
		public int LowerLimit { get; set; }
		public int UpperLimit { get; set; }
		public string By { get; set; }
		public string StoreInKey { get; set; }
		public string Get { get; set; }

		public object [] ToArgs ()
		{
			System.Collections.ArrayList args = new System.Collections.ArrayList();

			if (LowerLimit != 0 || UpperLimit != 0) {
				args.Add ("LIMIT");
				args.Add (LowerLimit);
				args.Add (UpperLimit);
			}
			if (Lexographically)
				args.Add("ALPHA");
			if (!string.IsNullOrEmpty (By)) {
				args.Add("BY");
				args.Add(By);
			}
			if (!string.IsNullOrEmpty (Get)) {
				args.Add("GET");
				args.Add(Get);
			}
			return args.ToArray ();
		}
	}
}
