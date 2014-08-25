run: test.exe
	mono --debug test.exe

test.exe: test.cs RedisClient.cs RedisSub.cs Redis.cs Makefile
	gmcs -debug -d:DEBUG test.cs RedisClient.cs RedisSub.cs Redis.cs
