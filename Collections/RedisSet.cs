//  RedisSet.cs : Abstracted Set commands for Redis
//  
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//   Jonathan R. Steele (jrsteele@gmail.com)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
using System;
using RedisSharp;

namespace RedisSharp.Collections {
	
	public class RedisSet<T> : RedisGenericBase<T>
	{
		internal RedisSet(string key, string host, int port) : base (key, host, port) { }
		
		public int Count {
			get {
				return SendDataExpectInt (null, "SCARD {0}\r\n", Key);
			}
		}
		
		public bool Add(T item)
		{
			byte[] data = Serialize(item);
			return SendDataExpectInt(data, "SADD {0} {1}\r\n", Key, data.Length) > 0;
		}
		
		public bool Remove(T item)
		{
			byte[] data = Serialize(item);
			return SendDataExpectInt(data, "SREM {0} {1}\r\n", Key, data.Length) > 0;
		}
		
		public T PopRandomItem()
		{
			byte[] result = SendExpectData(null,"SPOP {0}\r\n", Key);
			return  DeSerialize(result);
		}
		
		public T RandomItem()
		{
			byte[] result = SendExpectData(null,"SRANDMEMBER {0}\r\n", Key);
			return  DeSerialize(result);
			
		}
		
		protected T[] SetCommand(string command, params string[] keys)
		{
			if (keys == null)
				throw new ArgumentNullException();
			
			System.Collections.Generic.List<T> items = new System.Collections.Generic.List<T>();
			
			byte[][] result = SendDataCommandExpectMultiBulkReply (null, command + " " + Key + " " + string.Join (" ", keys) + "\r\n");
			foreach (byte[] b in result) {
				items.Add(DeSerialize(b));
			}
			
			
			return items.ToArray();
			
		}
		
		protected RedisSet<T> StoreSetCommand(string command, string destKey, params string[] keys)
		{
			if (keys == null)
				throw new ArgumentNullException();
			
			SendExpectSuccess( command + " " + destKey + " " + Key + " " + string.Join(" ", keys) + "\r\n");
			return new RedisSet<T>(destKey, Host, Port);
		}
		
		public T[] Union(params string[] keys)
		{
			return SetCommand("SUNION",keys);
		}
		
		public RedisSet<T> StoreUnion(string destKey, params string[] keys)
		{
			return StoreSetCommand("SUNIONSTORE", destKey , keys);
		}
		
		public T[] Intersect(params string[] keys)
		{
			return SetCommand("SINTER", keys);
		}
		
		public RedisSet<T> StoreIntersect(string destKey, params string[] keys)
		{
			return StoreSetCommand("SINTERSTORE",destKey, keys);
		}
		
		public T[] Difference(params string[] keys)
		{
			return SetCommand("SDIFF", keys);
		}
		
		public RedisSet<T> StoreDifference(string destKey, params string[] keys)
		{
			return StoreSetCommand("SDIFFSTORE", destKey, keys);
		}
		

	}
	
}

