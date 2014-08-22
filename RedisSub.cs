//
// RedisSub.cs: ECMA CLI Binding for Redis pub/sub
//
// Licensed under the same terms of Redis: new BSD license.
//

using System;
using System.Text;
using System.Threading;

namespace RedisSharp {
	public delegate void RedisSubEventHandler (object sender, RedisSubEventArgs e);

	public class RedisSubEventArgs : EventArgs
	{
		public string kind;
		public string pattern;
		public string channel;
		public object message;  // a byte [] or an integer depending on kind
	}

	public class RedisSub : Redis {
		Thread worker = null;
		bool continueWorking = false;

		public RedisSub (string host, int port) : base (host, port)
		{
		}

		public RedisSub (string host) : base (host)
		{
		}

		public RedisSub () : base ()
		{
		}

		#region Redis commands
		public bool Subscribe (params string [] channels)
		{
			return SendSubCommand ("SUBSCRIBE", channels);
		}

		public bool PSubscribe (params string [] patterns)
		{
			return SendSubCommand ("PSUBSCRIBE", patterns);
		}

		public bool Unsubscribe (params string [] channels)
		{
			return SendSubCommand ("UNSUBSCRIBE", channels);
		}

		public bool PUnsubscribe (params string [] patterns)
		{
			return SendSubCommand ("PUNSUBSCRIBE", patterns);
		}
		#endregion

		#region Event handlers
		public event RedisSubEventHandler MessageReceived;
		public event RedisSubEventHandler SubscribeReceived;
		public event RedisSubEventHandler UnsubscribeReceived;

		protected virtual void OnMessageReceived (RedisSubEventArgs e)
		{
			if (MessageReceived != null)
				MessageReceived (this, e);
		}

		protected virtual void OnSubscribeReceived (RedisSubEventArgs e)
		{
			if (SubscribeReceived != null)
				SubscribeReceived (this, e);
		}

		protected virtual void OnUnsubscribeReceived (RedisSubEventArgs e)
		{
			if (UnsubscribeReceived != null)
				UnsubscribeReceived (this, e);
		}
		#endregion

		/// <summary>
		/// Send a (un)subscribe command and start/stop listening.
		/// </summary>
		protected bool SendSubCommand (string cmd, params string [] args)
		{
			if (SendCommand (cmd, args))
			{
				// command was sent; make sure we are listening
				if (!continueWorking)
				{
					Log ("C", "start listening");
					continueWorking = true;
					worker = new Thread (SubWorker);
					worker.Start();
				}
			}
			else
			{
				// send failed; stop listening
				if (continueWorking)
				{
					Log ("C", "stop listening");
					continueWorking = false;
					worker.Join();
					worker = null;
				}
			}
			return continueWorking;
		}

		/// <summary>
		/// Worker method to handle incoming messages from Redis.
		/// </summary>
		void SubWorker()
		{
			byte [][] data = null;
			while (continueWorking)
			{
				try
				{
					data = ReadDataArray ();
				}
				catch (Exception ex)
				{
					if (continueWorking)
						throw ex;
					else
						break;
				}
				if (data.Length < 3)
					throw new Exception ("Received unexpected message with " +
						data.Length + " elements");
				RedisSubEventArgs e = new RedisSubEventArgs();
				e.kind = Encoding.UTF8.GetString (data[0]);
				switch (e.kind) {
				case "message":
					if (MessageReceived == null) break;
					e.channel = Encoding.UTF8.GetString (data [1]);
					e.message = data [2];
					OnMessageReceived(e);
					break;
				case "subscribe":
					if (SubscribeReceived == null) break;
					e.channel = Encoding.UTF8.GetString (data [1]);
					e.message = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnSubscribeReceived(e);
					break;
				case "unsubscribe":
					if (UnsubscribeReceived == null) break;
					e.channel = Encoding.UTF8.GetString (data [1]);
					e.message = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnUnsubscribeReceived(e);
					break;
				case "pmessage":
					if (MessageReceived == null) break;
					if (data.Length != 4)
						throw new Exception ("Received invalid pmessage with " +
							data.Length + " elements");
					e.pattern = Encoding.UTF8.GetString (data [1]);
					e.channel = Encoding.UTF8.GetString (data [2]);
					e.message = data [3];
					OnMessageReceived(e);
					break;
				case "psubscribe":
					if (SubscribeReceived == null) break;
					e.pattern = Encoding.UTF8.GetString (data [1]);
					e.message = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnSubscribeReceived(e);
					break;
				case "punsubscribe":
					if (UnsubscribeReceived == null) break;
					e.pattern = Encoding.UTF8.GetString (data [1]);
					e.message = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnUnsubscribeReceived(e);
					break;
				default:
					throw new Exception ("Received message of unsupported kind " + e.kind);
				}
			}
		}

		protected override void Dispose (bool disposing)
		{
			if (disposing)
			{
				continueWorking = false;
				base.Dispose (true); // close socket
				if (worker != null)
				   worker.Join();
				worker = null;
			}
			else
				base.Dispose(false);
		}
	}
}
