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

        r.RPush("alist", "avalue");
        r.RPush("alist", "another value");
        if (r.ListLength("alist") != 2)
            Console.WriteLine("error: List length should have been 2");
        var value = Encoding.UTF8.GetString(r.ListIndex("alist", 1));
        if(!value.Equals("another value"))
          Console.WriteLine("error: Received {0} and should have been 'another value'", value);
	}
}