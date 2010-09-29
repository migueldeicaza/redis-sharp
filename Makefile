run: test.exe
	mono --debug test.exe

test.exe: test.cs RedisBase.cs Redis.cs Subscriber.cs Makefile
	gmcs -debug test.cs RedisBase.cs Redis.cs Subscriber.cs
