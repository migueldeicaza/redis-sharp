//  RedisGenericBase.cs : Generic methods for Abstracted Collections
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace RedisSharp.Collections {
	public abstract class RedisGenericBase<T> : RedisBase
	{
		internal RedisGenericBase(string key, string host, int port) : base (host, port) { Key = key; }
		
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
	
		
	}
}

