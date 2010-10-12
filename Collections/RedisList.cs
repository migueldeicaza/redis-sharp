//  RedisList.cs : Abstracted List Implementations for Redis
//  
//  Author:
//       Jonathan R. Steele (jrsteele@gmail.com)
//  
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace RedisSharp.Collections {
	
	public class RedisList<T> : RedisBase
	{
		internal RedisList(string key, string host, int port) : base (host, port) { Key = key; }
		
		public string Key {
			get;
			private set;
		}
		
		protected T DeSerialize(byte[] data)
		{
			IFormatter bFormatter = new BinaryFormatter();
			MemoryStream ms = new MemoryStream(data);
			
			T result =  (T)bFormatter.Deserialize(ms);
			
			ms.Dispose();
			ms.Close();
			
			return result;
			
		}
		
		protected byte[] Serialize(T data)
		{
			IFormatter bFormatter = new BinaryFormatter();
			MemoryStream ms = new MemoryStream();
			
			bFormatter.Serialize(ms, data);
			
			ms.Position = 0;
			
			byte[] result = ms.ToArray();
			
			ms.Dispose();
			ms.Close();
			
			return result;
		}
	
		
		public T this[int index] {
			get {
				SendCommand ("LINDEX {0} {1}\r\n", Key, index);
				byte[] data = ReadData();
				
				return DeSerialize(data);
			}
			set {
				byte[] data = Serialize(value);
				SendDataExpectInt(data, "LSET {0} {1} {2}\r\n", Key, index, data.Length);
			}
			
		}
		
		public int Length {
			get {
				return SendExpectInt("LLEN {0}\r\n", Key);
			}
		}
		
		public void Add(T item)
		{
			byte[] data = Serialize(item);
			SendDataExpectInt(data, "RPUSH {0} {1}\r\n", Key, data.Length);
		}
		
		public void Remove(T item)
		{
			byte[] data = Serialize(item);
			SendDataExpectInt(data, "LREM {0} 1 {1}\r\n", Key, data.Length);
		}
		
		public void RemoveAt(int index)
		{
			T itm = this[index];
			
			if (itm.Equals(default(T))) return;
			
			Remove(itm);
			
		}
		
		public void Clear()
		{
			SendExpectInt("DEL {0}\r\n", Key);
			
		}
		
		public T[] GetRange(int start, int end)
		{
			List<T> data = new List<T>();
			
			byte[][] result = SendDataCommandExpectMultiBulkReply(null, "LRANGE {0} {1} {2}\r\n", Key, start, end);
			
			foreach (byte[] itm in result) 
				data.Add(DeSerialize(itm));
			
			result = null;
			
			return data.ToArray();
			
		}
	}
	
	
	
}
