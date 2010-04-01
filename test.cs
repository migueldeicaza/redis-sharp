using System;
using System.Text;
using System.Collections.Generic;

class Test {
	static void Main (string[] args)
	{
        Redis r;
        if (args.Length >= 2)
        {
            r = new Redis(args[0], Convert.ToInt16(args[1]));
        } else {
            r = new Redis();
        }

        r.Set("foo", "bar");
        r.FlushAll();
        if (r.Keys.Length > 0)
            Console.WriteLine("error: there should be no keys but there were {0}", r.Keys.Length);
    	r.Set ("foo", "bar");
		if (r.Keys.Length < 1)
			Console.WriteLine ("error: there should be at least one key");
		if (r.GetKeys ("f*").Length < 1)
			Console.WriteLine ("error: there should be at least one key");
		
		if (r.TypeOf ("foo") != Redis.KeyType.String)
			Console.WriteLine ("error: type is not string");
		r.Set ("bar", "foo");

		var arr = r.GetKeys ("foo", "bar");
		if (arr.Length != 2)
			Console.WriteLine ("error, expected 2 values");
		if (arr [0].Length != 3)
			Console.WriteLine ("error, expected foo to be 3");
		if (arr [1].Length != 3)
			Console.WriteLine ("error, expected bar to be 3");
		
		r ["one"] = "world";
		if (r.GetSet ("one", "newvalue") != "world")
			Console.WriteLine ("error: Getset failed");
		if (!r.Rename ("one", "two"))
			Console.WriteLine ("error: failed to rename");
		if (r.Rename ("one", "one"))
			Console.WriteLine ("error: should have sent an error on rename");
		r.Db = 10;
		r.Set ("foo", "diez");
		if (r.GetString ("foo") != "diez"){
			Console.WriteLine ("error: got {0}", r.GetString ("foo"));
		}
		if (!r.Remove ("foo"))
			Console.WriteLine ("error: Could not remove foo");
		r.Db = 0;
		if (r.GetString ("foo") != "bar")
			Console.WriteLine ("error, foo was not bar");
		if (!r.ContainsKey ("foo"))
			Console.WriteLine ("error, there is no foo");
		if (r.Remove ("foo", "bar") != 2)
			Console.WriteLine ("error: did not remove two keys");
		if (r.ContainsKey ("foo"))
			Console.WriteLine ("error, foo should be gone.");
		r.Save ();
		r.BackgroundSave ();
		Console.WriteLine ("Last save: {0}", r.LastSave);
		//r.Shutdown ();

		var info = r.GetInfo ();
		foreach (var k in info.Keys){
			Console.WriteLine ("{0} -> {1}", k, info [k]);
		}

		var dict = new Dictionary<string, byte[]>();
		dict ["hello"] = Encoding.UTF8.GetBytes ("world");
		dict ["goodbye"] = Encoding.UTF8.GetBytes ("my dear");
		
		//r.Set (dict);

        r.RightPush("alist", "avalue");
        r.RightPush("alist", "another value");
        assert(r.ListLength("alist") == 2, "List length should have been 2");

        var value = Encoding.UTF8.GetString(r.ListIndex("alist", 1));
        if(!value.Equals("another value"))
          Console.WriteLine("error: Received {0} and should have been 'another value'", value);
        value = Encoding.UTF8.GetString(r.LeftPop("alist"));
        if (!value.Equals("avalue"))
            Console.WriteLine("error: Received {0} and should have been 'avalue'", value);
        if (r.ListLength("alist") != 1)
            Console.WriteLine("error: List should have one element after pop");
        r.RightPush("alist", "yet another value");
        byte[][] values = r.ListRange("alist", 0, 1);
        if (!Encoding.UTF8.GetString(values[0]).Equals("another value"))
            Console.WriteLine("error: Range did not return the right values");

        assert(r.AddToSet("FOO", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
        assert(r.AddToSet("FOO", Encoding.UTF8.GetBytes("BAZ")),"Problem adding to set");
        assert(r.AddToSet("FOO", "Hoge"),"Problem adding string to set");
        assert(r.CardinalityOfSet("FOO") == 3, "Cardinality should have been 3 after adding 3 items to set");
        assert(r.IsMemberOfSet("FOO", Encoding.UTF8.GetBytes("BAR")), "BAR should have been in the set");
        assert(r.IsMemberOfSet("FOO", "BAR"), "BAR should have been in the set");
        byte[][] members = r.GetMembersOfSet("FOO");
        assert(members.Length == 3, "Set should have had 3 members");

        assert(r.RemoveFromSet("FOO", "Hoge"), "Should have removed Hoge from set");
        assert(!r.RemoveFromSet("FOO", "Hoge"), "Hoge should not have existed to be removed");
        assert(2 == r.GetMembersOfSet("FOO").Length, "Set should have 2 members after removing Hoge");
		
		assert(r.AddToSet("BAR", Encoding.UTF8.GetBytes("BAR")), "Problem adding to set");
        assert(r.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM1")),"Problem adding to set");
        assert(r.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM2")),"Problem adding string to set");
		
		assert(r.GetUnionOfSets("FOO","BAR").Length == 4, "Resulting union should have 4 items");
		assert(1 == r.GetIntersectionOfSets("FOO", "BAR").Length, "Resulting intersection should have 1 item");
		assert(1 == r.GetDifferenceOfSets("FOO", "BAR").Length, "Resulting difference should have 1 item");
		assert(2 == r.GetDifferenceOfSets("BAR", "FOO").Length, "Resulting difference should have 2 items");
		
		byte[] itm = r.GetRandomMemberOfSet("FOO");
		assert(null != itm, "GetRandomMemberOfSet should have returned an item");
		assert(r.MoveMemberToSet("FOO","BAR", itm), "Data within itm should have been moved to set BAR");
		
		
		r.FlushDb();
		
		 if (r.Keys.Length > 0)
            Console.WriteLine("error: there should be no keys but there were {0}", r.Keys.Length);
		
		
	}

    static void assert(bool condition, string message)
    {
        if (!condition)
        {
            Console.WriteLine("error: {0}", message);
        }
    }
}