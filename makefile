CC=gcc
MYFLAGS =  -g -O0 -Wall -fno-builtin-malloc -fno-builtin-free -fno-builtin-realloc -fno-builtin-calloc

all: libSimpleMTMM.a

libSimpleMTMM.a: mtmm.c
	$(CC) $(MYFLAGS) -c mtmm.c 
	ar rcu libSimpleMTMM.a mtmm.o
	ranlib libSimpleMTMM.a
