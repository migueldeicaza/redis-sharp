//  RedisExtensions.cs : Extension methods for Redis Client Library
//  
//  Author:
//       Jonathan R. Steele (jrsteele@gmail.com)
//  
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
using System;
using System.Linq;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace RedisSharp.Extensions {
	
	public static class RedisExtensions
	{
		public static T Get<T>(this Redis self, string key)
		{
			T result = default(T);
			
			byte[] data = self.Get(key);
			
			if (data == null)
				return result;
			
			MemoryStream ms = new MemoryStream(data);
			IFormatter bFormatter = new BinaryFormatter();
			
			result = (T)bFormatter.Deserialize(ms);
			
			ms.Close();
			ms.Dispose();

			return result;
		}
		
				
		public static void Set<T>(this Redis self, string key, T data)
		{
			IFormatter bFormatter = new BinaryFormatter();
			MemoryStream memStream = new MemoryStream();
			
			bFormatter.Serialize(memStream,data);
			
			memStream.Position = 0;
			
			byte[] serliazedData = memStream.ToArray();
			
			memStream.Close();
			memStream.Dispose();
			
			self.Set(key, serliazedData);
			
		}
		
		public static void Set<T>(this Redis self, string key, DateTime expiration, T data)
		{
			self.Set<T>(key, data);
			
			self.Expire(key, (int)(expiration - DateTime.Now).TotalSeconds);
		}
		
	}
}

