using System;
using System.Text;
using System.Collections.Generic;

class Test {

	static int nPassed = 0;
	static int nFailed = 0;

	static void Main (string[] args)
	{
		Redis r;
                string s;
                int i;

		if (args.Length >= 2)
			r = new Redis(args[0], Convert.ToInt16(args[1]));
		else if (args.Length >= 1)
			r = new Redis(args[0]);
		else
			r = new Redis();

		r.Set("foo", "bar");
		r.FlushAll();
		assert ((i = r.Keys.Length) == 0, "there should be no keys but there were {0}", i);
		r.Set ("foo", "bar");
		assert ((i = r.Keys.Length) == 1, "there should be one key but there were {0}", i);
		r.Set ("foo bär", "bär foo");
		assert ((i = r.Keys.Length) == 2, "there should be two keys but there were {0}", i);
		
		assert (r.TypeOf ("foo") == Redis.KeyType.String, "type is not string");
		r.Set ("bar", "foo");

		byte [][] arr = r.MGet ("foo", "bar", "foo bär");
		assert (arr.Length == 3, "expected 3 values");
		assert ((s = Encoding.UTF8.GetString (arr [0])) == "bar",
			"expected \"foo\" to be \"bar\", got \"{0}\"", s);
		assert ((s = Encoding.UTF8.GetString (arr [1])) == "foo",
			"expected \"bar\" to be \"foo\", got \"{0}\"", s);
		assert ((s = Encoding.UTF8.GetString (arr [2])) == "bär foo",
			"expected \"foo bär\" to be \"bär foo\", got \"{0}\"", s);
		
		r ["{one}"] = "world";
		assert (r.GetSet ("{one}", "newvalue") == "world", "GetSet failed");
		assert (r.Rename ("{one}", "two"), "failed to rename");
		assert (!r.Rename ("{one}", "{one}"), "should have sent an error on rename");
		r.Set("binary", new byte[] { 0x00, 0x8F });
		assert((i = r.Get("binary").Length) == 2, "expected 2 bytes, got {0}", i);
		r.Db = 10;
		r.Set ("foo", "diez");
		assert ((s = r.GetString ("foo")) == "diez", "got {0}", s);
		assert (r.Remove ("foo"), "could not remove foo");
		r.Db = 0;
		assert (r.GetString ("foo") == "bar", "foo was not bar");
		assert (r.ContainsKey ("foo"), "there is no foo");
		assert (r.Remove ("foo", "bar") == 2, "did not remove two keys");
		assert (!r.ContainsKey ("foo"), "foo should be gone.");
		r.Save ();
		r.BackgroundSave ();
		Console.WriteLine ("Last save: {0}", r.LastSave);
		//r.Shutdown ();

		var info = r.GetInfo ();
		foreach (var k in info.Keys) {
			Console.WriteLine ("{0} -> {1}", k, info [k]);
		}

		var dict = new Dictionary<string, byte[]>();
		dict ["hello"] = Encoding.UTF8.GetBytes ("world");
		dict ["goodbye"] = Encoding.UTF8.GetBytes ("my dear");
		dict ["schön"] = Encoding.UTF8.GetBytes("grün");
		
		r.Set (dict);

		assert ((s = r.GetString("hello")) == "world", "got \"{0}\"", s);
		assert ((s = r.GetString("goodbye")) == "my dear", "got \"{0}\"", s);
		assert ((s = r.GetString("schön")) == "grün", "got \"{0}\"", s);

		r.RightPush("alist", "avalue");
		r.RightPush("alist", "another value");
		assert (r.ListLength("alist") == 2, "List length should have been 2");

		string value = Encoding.UTF8.GetString(r.ListIndex("alist", 1));
		if(!value.Equals("another value"))
			Console.WriteLine("error: Received {0} and should have been 'another value'", value);
		value = Encoding.UTF8.GetString(r.LeftPop("alist"));
		if (!value.Equals("avalue"))
			Console.WriteLine("error: Received {0} and should have been 'avalue'", value);
		if (r.ListLength("alist") != 1)
			Console.WriteLine("error: List should have one element after pop");
		r.RightPush("alist", "yet another value");
		byte[][] values = r.ListRange("alist", 0, 1);
		assert (Encoding.UTF8.GetString(values[0]).Equals("another value"),
			"range did not return the right values");

		assert (r.AddToSet("FOO", Encoding.UTF8.GetBytes("BAR")), "problem adding to set");
		assert (r.AddToSet("FOO", Encoding.UTF8.GetBytes("BAZ")), "problem adding to set");
		assert (r.AddToSet("FOO", "Hoge"), "problem adding string to set");
		assert (r.CardinalityOfSet("FOO") == 3, "cardinality should have been 3 after adding 3 items to set");
		assert (r.IsMemberOfSet("FOO", Encoding.UTF8.GetBytes("BAR")), "BAR should have been in the set");
		assert (r.IsMemberOfSet("FOO", "BAR"), "BAR should have been in the set");
		byte[][] members = r.GetMembersOfSet("FOO");
		assert (members.Length == 3, "set should have had 3 members");

		assert (r.RemoveFromSet("FOO", "Hoge"), "should have removed Hoge from set");
		assert (!r.RemoveFromSet("FOO", "Hoge"), "Hoge should not have existed to be removed");
		assert (2 == r.GetMembersOfSet("FOO").Length, "set should have 2 members after removing Hoge");
		
		assert (r.AddToSet("BAR", Encoding.UTF8.GetBytes("BAR")), "problem adding to set");
		assert (r.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM1")), "problem adding to set");
		assert (r.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM2")), "problem adding string to set");
		
		assert (r.GetUnionOfSets("FOO","BAR").Length == 4, "resulting union should have 4 items");
		assert (1 == r.GetIntersectionOfSets("FOO", "BAR").Length, "resulting intersection should have 1 item");
		assert (1 == r.GetDifferenceOfSets("FOO", "BAR").Length, "resulting difference should have 1 item");
		assert (2 == r.GetDifferenceOfSets("BAR", "FOO").Length, "resulting difference should have 2 items");
		
		byte[] itm = r.GetRandomMemberOfSet("FOO");
		assert (null != itm, "GetRandomMemberOfSet should have returned an item");
		assert (r.MoveMemberToSet("FOO","BAR", itm), "data within itm should have been moved to set BAR");

		r.FlushDb();

		assert ((i = r.Keys.Length) == 0, "there should be no keys but there were {0}", i);

		Console.WriteLine("\nPassed tests: {0}\nFailed tests: {1}", nPassed, nFailed);
	}

	static void assert (bool condition, string message, params object [] args)
	{
		if (condition)
		{
			nPassed ++;
		}
		else
		{
			nFailed ++;
			Console.Error.WriteLine("error: " + message, args);
		}
	}
}
