//
// RedisSub.cs: ECMA CLI Binding for Redis pub/sub
//
// Licensed under the same terms of Redis: new BSD license.
//

using System;
using System.Text;
using System.Threading;

namespace RedisSharp {
	public delegate void MessageEventHandler (object sender, MessageEventArgs e);
	public delegate void SubscribeEventHandler (object sender, SubscribeEventArgs e);
	public delegate void UnsubscribeEventHandler (object sender, UnsubscribeEventArgs e);
	public delegate void PMessageEventHandler (object sender, PMessageEventArgs e);
	public delegate void PSubscribeEventHandler (object sender, PSubscribeEventArgs e);
	public delegate void PUnsubscribeEventHandler (object sender, PUnsubscribeEventArgs e);

	public class RedisSubEventArgs : EventArgs
	{
		public string channel;
	}

	public class MessageEventArgs : RedisSubEventArgs
	{
		public byte [] message;
	}

	public class SubscribeEventArgs : RedisSubEventArgs
	{
		public int nTotal; // number of channels we are subscribed to
	}

	public class UnsubscribeEventArgs : RedisSubEventArgs
	{
		public int nTotal;
	}

	public class PMessageEventArgs : MessageEventArgs
	{
		public string pattern;
	}

	public class RedisPSubEventArgs : RedisSubEventArgs
	{
		public string pattern;
	}

	public class PSubscribeEventArgs : RedisPSubEventArgs
	{
		public int nTotal;
	}

	public class PUnsubscribeEventArgs : RedisPSubEventArgs
	{
		public int nTotal;
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
		public event MessageEventHandler MessageReceived;
		public event SubscribeEventHandler SubscribeReceived;
		public event UnsubscribeEventHandler UnsubscribeReceived;
		public event PMessageEventHandler PMessageReceived;
		public event PSubscribeEventHandler PSubscribeReceived;
		public event PUnsubscribeEventHandler PUnsubscribeReceived;

		protected virtual void OnMessageReceived (MessageEventArgs e)
		{
			if (MessageReceived != null)
				MessageReceived (this, e);
		}

		protected virtual void OnSubscribeReceived (SubscribeEventArgs e)
		{
			if (SubscribeReceived != null)
				SubscribeReceived (this, e);
		}

		protected virtual void OnUnsubscribeReceived (UnsubscribeEventArgs e)
		{
			if (UnsubscribeReceived != null)
				UnsubscribeReceived (this, e);
		}

		protected virtual void OnPMessageReceived (PMessageEventArgs e)
		{
			if (PMessageReceived != null)
				PMessageReceived (this, e);
		}

		protected virtual void OnPSubscribeReceived (PSubscribeEventArgs e)
		{
			if (PSubscribeReceived != null)
				PSubscribeReceived (this, e);
		}

		protected virtual void OnPUnsubscribeReceived (PUnsubscribeEventArgs e)
		{
			if (PUnsubscribeReceived != null)
				PUnsubscribeReceived (this, e);
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
				string kind = Encoding.UTF8.GetString (data[0]);
				switch (kind) {
				case "message":
					if (MessageReceived == null) break;
					MessageEventArgs em = new MessageEventArgs();
					em.channel = Encoding.UTF8.GetString (data [1]);
					em.message = data [2];
					OnMessageReceived(em);
					break;
				case "subscribe":
					if (SubscribeReceived == null) break;
					SubscribeEventArgs es = new SubscribeEventArgs();
					es.channel = Encoding.UTF8.GetString (data [1]);
					es.nTotal = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnSubscribeReceived(es);
					break;
				case "unsubscribe":
					if (UnsubscribeReceived == null) break;
					UnsubscribeEventArgs eu = new UnsubscribeEventArgs();
					eu.channel = Encoding.UTF8.GetString (data [1]);
					eu.nTotal = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnUnsubscribeReceived(eu);
					break;
				case "pmessage":
					if (PMessageReceived == null) break;
					if (data.Length != 4)
						throw new Exception ("Received invalid pmessage with " +
							data.Length + " elements");
					PMessageEventArgs ep = new PMessageEventArgs();
					ep.pattern = Encoding.UTF8.GetString (data [1]);
					ep.channel = Encoding.UTF8.GetString (data [2]);
					ep.message = data [3];
					OnPMessageReceived(ep);
					break;
				case "psubscribe":
					if (PSubscribeReceived == null) break;
					PSubscribeEventArgs eps = new PSubscribeEventArgs();
					eps.pattern = Encoding.UTF8.GetString (data [1]);
					eps.channel = "";
					eps.nTotal = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnPSubscribeReceived(eps);
					break;
				case "punsubscribe":
					if (PUnsubscribeReceived == null) break;
					PUnsubscribeEventArgs epu = new PUnsubscribeEventArgs();
					epu.pattern = Encoding.UTF8.GetString (data [1]);
					epu.channel = "";
					epu.nTotal = int.Parse(Encoding.UTF8.GetString (data [2]));
					OnPUnsubscribeReceived(epu);
					break;
				default:
					throw new Exception ("Received message of unsupported kind " + kind);
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
