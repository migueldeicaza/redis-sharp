//  RedisSet.cs
//  
//  Author:
//       Jonathan R. Steele <jsteele@sabresystems.com>
//  
//  Copyright (c) 2010 Sabre Systems, Inc
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
		
		public T[] UnionOfSets(params string[] keys)
		{
			System.Collections.Generic.List<T> items = new System.Collections.Generic.List<T>();
			
			byte[][] result = SendDataCommandExpectMultiBulkReply (null, "SUNION " + Key + " " + string.Join (" ", keys) + "\r\n");
			foreach (byte[] b in result) {
				items.Add(DeSerialize(b));
			}
			
			
			return items.ToArray();
		}
		
		public RedisSet<T> StoreUnionOfsets(string destKey, params string[] keys)
		{
			SendExpectSuccess("SUNIONSTORE " + destKey + " " + string.Join(" ", keys) + "\r\n");
			return new RedisSet<T>(destKey, Host, Port);
		}
		
		

	}
	
}

