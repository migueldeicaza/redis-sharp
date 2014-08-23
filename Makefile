run: test.exe
	mono --debug test.exe

test.exe: test.cs RedisComm.cs RedisSub.cs Redis.cs Makefile
	gmcs -debug test.cs RedisComm.cs RedisSub.cs Redis.cs
