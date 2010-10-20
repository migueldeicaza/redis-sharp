run.exe: test.exe 
	mono test.exe
test.exe: test.cs RedisSharp.dll
	gmcs -debug -r:RedisSharp.dll test.cs

RedisSharp.dll: RedisBase.cs Redis.cs Subscriber.cs RedisExtensions.cs Collections/* RedisSharp.snk
	gmcs -debug -target:library -out:RedisSharp.dll -keyfile:RedisSharp.snk RedisBase.cs Redis.cs Subscriber.cs RedisExtensions.cs Collections/* 

clean: 
	rm test.exe
	rm test.exe.mdb
	rm RedisSharp.dll
	rm RedisSharp.dll.mdb
