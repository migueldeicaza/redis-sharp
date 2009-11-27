run: test.exe
	mono --debug test.exe

test.exe: test.cs redis-sharp.cs Makefile
	gmcs -debug test.cs redis-sharp.cs