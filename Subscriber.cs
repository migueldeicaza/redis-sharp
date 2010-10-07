//
// Subscriber.cs: ECMA CLI Binding to the Redis key-value storage system
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
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Linq;

namespace RedisSharp {
	internal class Subscriber  : RedisBase {
			
		System.Threading.Thread worker;
		Dictionary<string,Action<byte[]>> callBacks;
		bool continueWorking;
	
		internal Subscriber(String host, int port) : base(host, port)
		{ }
		
		public void Add(string channel, Action<byte[]> callBack)
		{
			AddToCallBack(channel, callBack);		
			
			if (!channel.Contains("*"))
				SendCommand("SUBSCRIBE {0}\r\n", channel);
			else
				SendCommand("PSUBSCRIBE {0}\r\n", channel);
			
		}
		
		public void Remove(string channel)
		{
			lock(callBacks) {
				if (callBacks == null) return;
				
				if (!callBacks.ContainsKey(channel)) return;
				
				callBacks.Remove(channel);
				
				continueWorking = (callBacks.Count > 0);
				
				if (channel.Contains("*"))
					SendCommand("punsubscribe {0}\r\n", channel);
				else
					SendCommand("unsubscribe {0}\r\n", channel);
	
			}
			
			/* Wait for the worker thread to finish up */
			if (!continueWorking) worker.Join();
			
		}
		
		public void RemoveAll()
		{
			continueWorking = false;
			SendCommand("UNSUBSCRIBE\r\n");
			worker.Join();
			
			callBacks.Clear();
			callBacks = null;
		}
		
		/// <summary>
		/// Worker ThreadStart method to handle incoming messages from Redis.
		/// </summary>
		void SubscritionWorker() 
		{
			byte[] message;
			string channel = String.Empty;
				
		
			while (continueWorking) 
			{
				message = ReadData();
		
				/* JS (09/27/2010):
				 * Determine the type of message coming in. 
				 * message correlates to something subscribed via the SUBSCRIBE command
				 * pmessage correlates to something subscribed via the PSUBSCRIBE command.
				 * 	(This will also give us the actual channel along with the pattern)
				 */
				switch (Encoding.ASCII.GetString(message)) {
					case "message":
						message = ReadData();	/* Channel */
						channel = Encoding.ASCII.GetString(message);
						message = ReadData(); /* Data */
						break;
					case "pmessage":
						message = ReadData(); /* Channel with mask */
						channel = Encoding.ASCII.GetString(message);
						ReadData(); /* This is the REAL channel, we don't care about that .. yet ;-) */
						message = ReadData(); /* Data */
						break;
					default:
						channel = string.Empty;
						break;
					
				}
				
				if (channel == string.Empty) continue;
				
				/* Determine which action we're calling */
				lock(callBacks) {
					callBacks[channel](message);	
					Log("Callback: {0}", channel);
				}
								
			}
			
		}
		
		
		/// <summary>
		/// Add an Action to the dictionary of callbacks
		/// </summary>
		void AddToCallBack(string channel, Action<byte[]> callBack) 
		{
			/* JS (09/26/2010): 
			 * If the dictionary of callbacks is null, create that, 
			 * and start the thread to listen for them 
			 */
			if (callBacks == null || callBacks.Count == 0) {
				RequireMinimumVersion("2.0.0");
				callBacks = new Dictionary<string, Action<byte[]>>();
				continueWorking = true;
				worker = new System.Threading.Thread(SubscritionWorker);
				worker.Start();
			}
			
			lock(callBacks) {
				if (callBacks.ContainsKey(channel)) return;
				callBacks.Add(channel, callBack);
			}
		}
		
		protected override void Dispose (bool disposing)
		{
			if (continueWorking) {
				RemoveAll();
			}
			base.Dispose (disposing);
			
			
		}
		
	}
}