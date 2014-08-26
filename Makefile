run: test.exe
	mono --debug test.exe

test.exe: test.cs redis-sharp.cs RedisSub.cs Makefile
	gmcs -debug -d:DEBUG test.cs redis-sharp.cs RedisSub.cs
