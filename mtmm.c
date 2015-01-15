/*
This module implements memory allocation with several CPUS(Hoard)
Each CPU has it's own heap which is composed of "superblocks"
There are size classes, which define the size of the blocks. Each superblock belongs to a size class and all it's blocks are of the same size.
There's also a global heap, used to store underpopulated superblocks. If a heap drops below a certain percentage of efficency, represented by the following invariant, a superblock is moved to the global heap in order to restore the invariant.
The invariant is: u > (1-f)a OR u > a - K*S, where u is the amount of allocated memory, a is the total amount of memory, f is the allowed empty fraction of the memory, K is a constant and S is the size of a superblock.
If the user needs a "large" block(more than half the size of a superblock), the allocation is done directly with the OS.
*/ 

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <pthread.h>
#include <math.h>
#include <string.h>
#include "mtmm.h"

#define NUM_OF_CLASSES 16
#define NUM_OF_CPUS 2
#define NUM_OF_HEAPS NUM_OF_CPUS + 1
#define SIZE_THRESHOLD SUPERBLOCK_SIZE/2
#define F 0.4					/*the empty fraction allowed in the invariant*/
#define K 0					/*the min number of superblocks in the invariant*/
#define SIZE_OF_CLASS(class) (1<<(class)) 	/*claculates the block size of a class(2^class)*/
#define EXIT(error) {printf(error); exit(1);}
#define HASH(id) (id)%NUM_OF_CPUS		/*the hash functions used for choosing a heap*/
#define PPRINT(str) {printf(str); fflush(stdout);}

/*TODO Remove inUse?*/
typedef struct sBlockHeader
{
	unsigned int blockSize;			/*the size of the block*/ 	
	int inUse;				/*is the block used*/
	
	struct sBlockHeader *next;		/*the next block in the superblock*/
	struct sSuperblockHeader *parentSuperblock; 	/*the block's superblock*/
} blockHeader;

typedef struct sBlockList
{
	blockHeader *head;			/*the first block in the list*/
} blockList;

/*TODO Remove numOfBlocks*/
typedef struct sSuperblockHeader
{
	unsigned int usedBlocks;		/*the number of used blocks in the superblock*/
	unsigned int numOfBlocks;		/*the number of blocks in the superblock*/
	blockList freeList;			/*the list of free blocks in the superblock*/
	pthread_mutex_t lock;			/*the superblocks' lock*/

	struct sSuperblockHeader *next;		/*the next superblock in the list*/
	struct sSuperblockHeader *prev;		/*the previous superblock in the list*/
	struct sHeap *parentHeap;		/*the superblock's heap*/
} superblockHeader;

typedef struct sSuperblockList
{
	superblockHeader *head;			/*the first superblock in the list*/
	superblockHeader *tail;			/*the second superblock in the list*/
} superblockList;

typedef struct sSizeClass
{
	unsigned int size;			/*the size of the class*/
	unsigned int usedBlocks;		/*the number of used blocks in the class*/
	unsigned int numOfBlocks;		/*the number of blocks in the class*/
	superblockList superblocks;		/*the class' superblocks, sorted by fullness*/
	pthread_mutex_t lock;			/*the class' lock*/
} sizeClass;

typedef struct sHeap
{
	unsigned int id;			/*the id of the heap's CPU(NUM_OF_HEAPS-1 will always be the global heap's id)*/
	sizeClass classes[NUM_OF_CLASSES];	/*the size classes in the heap*/
} memHeap;

static int isInitialized = 0;			/*whether the data structure has been initialized*/
static memHeap heaps[NUM_OF_HEAPS];		/*1 heap per CPU and 1 additional global heap*/

/*initialize the data structure(runs only on the first malloc)*/
static void init()
{
	int i, j;
	for(i=0; i<NUM_OF_HEAPS; i++)
	{
		heaps[i].id = i;
		for(j=0; j<NUM_OF_CLASSES; j++)
		{
			heaps[i].classes[j].size = SIZE_OF_CLASS(j);
			heaps[i].classes[j].usedBlocks = 0;
			heaps[i].classes[j].numOfBlocks = 0;
			if(pthread_mutex_init(&heaps[i].classes[j].lock, NULL))
				EXIT("Mutex init failed")
		}
	}
}

/*request memory from OS*/
static void * fetch_memory(size_t sz)
{
	int fd;
	void *p;
	fd = open("/dev/zero", O_RDWR);
	if (fd == -1){
		perror(NULL);
		return NULL;
	}
	p = mmap(0, sz, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
	close(fd);
	if (p == MAP_FAILED){
		perror(NULL);
		return NULL;
	}
	return p;
}

/*Search the superblocks of a size class for a free block.
Returns NULL if not found*/
static blockHeader * search_sizeclass(sizeClass *class)
{
	if(class->usedBlocks == class->numOfBlocks) /*no available blocks*/
		return NULL;
	superblockHeader *p = (class->superblocks).head;
	while(p != NULL)
	{
		if(p->usedBlocks < p->numOfBlocks) /*there's a free block*/
			return (p->freeList).head; /*the first free block*/
		p = p->next;
	}
	return NULL; /*this shouldn't be reached, because we move past the first if(line 116) iff there's a free block.
			the first if isn't necessary but it avoids unnecessary scans*/
}

/*swap sb with the next superblock in a size class' list*/
static void swap_superblocks(superblockList *list, superblockHeader *sb)
{
	superblockHeader *nextSB = sb->next;
	if(nextSB != NULL)
	{
		if(sb->prev != NULL)
			(sb->prev)->next = nextSB;
		if(nextSB->next !=NULL)
			(nextSB->next)->prev = sb;
	
		superblockHeader *tmp_prev = sb->prev;
		sb->next = nextSB->next;
		sb->prev = nextSB;
		nextSB->next = sb;
		nextSB->prev = tmp_prev;
		if(list->head == sb)
			list->head = nextSB;
		if(list->tail == nextSB)
			list->tail = sb;
	}
}

/*move a superblock from one heap to another*/
static void move_superblock(superblockHeader *sb, memHeap *src, memHeap *dst, int class)
{
	sizeClass *src_class = &(src->classes[class]);
	sizeClass *dst_class = &(dst->classes[class]);
	superblockList *src_list = &(src_class->superblocks);
	superblockList *dst_list = &(dst_class->superblocks);
	/*remove superblock from the original list*/
	if(src_list->head == sb)
		src_list->head = sb->next;
	if(src_list->tail == sb)
		src_list ->tail = sb->prev;
	if(sb->next != NULL)
		(sb->next)->prev = sb->prev;
	if(sb->prev != NULL)
		(sb->prev)->next = sb->next;
	/*add sb to the head of the destination*/
	sb->prev = NULL;
	sb->next = dst_list->head;
	if(dst_list->head !=NULL)
		(dst_list->head)->prev = sb;
	else
		dst_list->tail = sb;
	dst_list->head = sb;
	/*move sb to it's appropriate place in the list according to its fullness*/
	while(sb->next != NULL && sb->usedBlocks < (sb->next)->usedBlocks)
	{
		swap_superblocks(dst_list,sb);
	}
	sb->parentHeap = dst;
	/*update statistics*/
	src_class->usedBlocks -= sb->usedBlocks;
	src_class->numOfBlocks -= sb->numOfBlocks;
	dst_class->usedBlocks += sb->usedBlocks;
	dst_class->numOfBlocks += sb->numOfBlocks;
}

/*initialize a superblock*/
static int init_superblock(superblockHeader *sb, int class)
{
	sb->usedBlocks = 0;
	/*in this implementation, the superblock header "steals" memory from the superblock, in order to keep the superblock size 64K. The block headers, however, don't "steal" from the block size because we want to be able to give the user up to 2^class bytes. therefore, the number of blocks in a super block is as following:
note:this does cause internal fragmentation inside the superblock(for example, a superblock from class 15 will have only 1 block!), but it does have the advantages listed above*/
	sb->numOfBlocks = (SUPERBLOCK_SIZE-sizeof(superblockHeader)) / (sizeof(blockHeader) + SIZE_OF_CLASS(class));
	if(pthread_mutex_init(&(sb->lock), NULL))
	{
		perror(NULL);
		return 1;
	}
	/*initialize the blocks*/
	(sb->freeList).head = (blockHeader*)(sb + 1);
	blockHeader *p = (sb->freeList).head;
	int i;
	for(i=0; i<sb->numOfBlocks; i++)
	{
		p->blockSize = SIZE_OF_CLASS(class);
		p->inUse = 0;	
		p->next = (blockHeader*)(((char*)(p) + sizeof(blockHeader) + SIZE_OF_CLASS(class)));
		p->parentSuperblock = sb;
		p=p->next;
	}
	return 0;
}

/*TODO Break into functions*/
/*First, the function searches a free block in the CPU's heap.
If there's none, it searches for one in the global heap.
If there's none there too, the function allocates a new superblock from the OS and puts it the the heap*/
void * malloc (size_t sz)
{
	/*if this is the first malloc, initialize the heaps*/
	if(!isInitialized)
	{
		init();
		isInitialized = 1;
	}
	
	/*handle allocations for "large" blocks, allocate the block directly from OS*/
	if(sz > SIZE_THRESHOLD)
	{
		blockHeader *p = (blockHeader *)fetch_memory(sz+sizeof(blockHeader));
		if(!p)
		{
			perror(NULL);
			return NULL;
		}
		p->blockSize = sz;
		return (p+1);
	}
	
	int class = (int) ceil(log2(sz)); /*the appropriate size class*/
	memHeap *heap = &(heaps[HASH(pthread_self())]);
	pthread_mutex_lock(&(heap->classes[class].lock)); /*lock the heap*/
	blockHeader *block = search_sizeclass(&(heap->classes[class])); /*search for a free block in the class*/
	if(block != NULL)
	{
		superblockHeader *superblock = block->parentSuperblock;
		blockList *list = &(superblock->freeList);
		/*remove the block from the free list(it's the head of the list)*/
		list->head = (list->head)->next;
		/*update the block's, superblock's and size class' statistics*/
		block -> inUse = 1;
		superblock->usedBlocks++;
		(heap->classes[class]).usedBlocks++;
		/*move the superblock to it's new correct position in the size class*/
		while(superblock->prev!=NULL && superblock->usedBlocks > (superblock->prev)->usedBlocks)
		{
			swap_superblocks(&((heap->classes[class]).superblocks),superblock->prev);
		}
		pthread_mutex_unlock(&(heap->classes[class].lock)); /*unlock the heap*/
		return (block + 1);
	}
	
	/*try to fetch a superblock from the global heap*/
	memHeap *globalHeap = &(heaps[NUM_OF_CPUS]);
	pthread_mutex_lock(&(globalHeap->classes[class].lock)); /*lock the global heap*/
	superblockHeader *superblock = (globalHeap->classes[class]).superblocks.head;
	if(superblock !=NULL) /*a superblock in the global heap must have empty space*/
	{
		blockList *list = &(superblock->freeList);
		blockHeader *block = list->head; 	/*a free block from the superblock*/
		list->head = (list->head)->next;	/*remove the block from the free list(it's the head of the list)*/	
		/*update the block's, superblock's and size class' statistics*/
		block -> inUse = 1;
		superblock->usedBlocks++;
		(globalHeap->classes[class]).usedBlocks++;
		/*move the superblock to the CPU heap*/
		move_superblock(superblock, globalHeap, heap, class);
		/*unlock the heaps*/
		pthread_mutex_unlock(&(globalHeap->classes[class].lock));
		pthread_mutex_unlock(&(heap->classes[class].lock));	
		return (block + 1);
	}
	
	/*allocate a new superblock from OS*/
	superblock = (superblockHeader *)fetch_memory(SUPERBLOCK_SIZE);
	init_superblock(superblock, class);
	if(superblock !=NULL)
	{
		superblock->parentHeap = heap;
		blockList *list = &(superblock->freeList);
		blockHeader *block = list->head; 	/*a free block from the superblock*/
		list->head = (list->head)->next;	/*remove the block from the free list*/
		/*update the block's, superblock's and size class' statistics*/
		block -> inUse = 1;
		superblock->usedBlocks++;
		sizeClass *sc = &(heap->classes[class]);
		sc->usedBlocks++;
		sc->numOfBlocks += superblock->numOfBlocks;
		/*put the superblock in the sizeclass*/
		superblock->parentHeap = heap;
		if(sc->superblocks.tail != NULL)
			(sc->superblocks.tail)->next = superblock;
		else
		{
			/*the size class is empty so this is also the first superblock*/
			sc->superblocks.head = superblock;
		}
		superblock->prev = sc->superblocks.tail;
		sc->superblocks.tail = superblock;
		superblock->next = NULL;
		/*move it to it's place*/
		while(superblock->prev!=NULL && superblock->usedBlocks > (superblock->prev)->usedBlocks)
		{
			swap_superblocks(&(sc->superblocks),superblock->prev);
		}
		pthread_mutex_unlock(&(heap->classes[class].lock));
		pthread_mutex_unlock(&(globalHeap->classes[class].lock));
		return (block + 1);
	}
	perror(NULL);
	return NULL;
}

/*The function frees the block, and preserves the invariant for the heap*/
void free (void * ptr) 
{
	if (ptr != NULL)
        {
		blockHeader *block = (blockHeader *)(ptr) - 1;
		if(block->blockSize > SUPERBLOCK_SIZE/2)
		{
			/*the block was directly allocated from OS*/
			if(munmap(block, block->blockSize + sizeof(blockHeader)))
				perror(NULL);
		}
		else
		{
			superblockHeader *sb = block->parentSuperblock;
			int class = log2(block->blockSize);
			memHeap *heap = sb->parentHeap;
			sizeClass *sc = &(heap->classes[class]);
			/*in order to lock the heap we need to lock the superblock first, or it could be moved*/
			pthread_mutex_lock(&(sb->lock));
			pthread_mutex_lock(&(sc->lock));
			pthread_mutex_unlock(&(sb->lock));

			/*free the block*/
			block->inUse = 0;
			block->next = sb->freeList.head;
			sb->freeList.head = block;

			/*update statistics*/
			sb->usedBlocks--;
			sc->usedBlocks--;

			/*move the superblock to it's appropriate location in the size class*/
			while(sb->next != NULL && sb->usedBlocks < (sb->next)->usedBlocks)
			{
				swap_superblocks(&(sc->superblocks),sb); 
			}

			memHeap *globalHeap = &(heaps[NUM_OF_CPUS]);

			/*preserve the invariant if the heap isn't the global heap*/
			if(heap != globalHeap && sc->usedBlocks < (sc->numOfBlocks - K*sb->numOfBlocks) && (float) (sc->usedBlocks) < (1-F)*(sc->numOfBlocks))
			{
				pthread_mutex_lock(&(globalHeap->classes[class].lock));
				superblockHeader *badSB = (sc->superblocks).tail; /*if the invariant is not kept, then there's a superblock that doesn't maintain it. The tail is the superblock with the least used blocks, and therefore can't maintain it*/	
				move_superblock(badSB, heap, globalHeap, class); /*move it to the global heap*/
				pthread_mutex_unlock(&(globalHeap->classes[class].lock));			
			}
			pthread_mutex_unlock(&(sc->lock));
		}
	}	
}

/*calloc is implemented because of a problem with linux-scalability(it used calloc which called the default malloc)*/
void *calloc(size_t num, size_t sz)
{
	void *p = malloc(num*sz);
	if(p!=NULL)
	{
		/*make everything 0*/
		memset(p, 0, num*sz);
	}
	return p;
}


void * realloc (void * ptr, size_t sz) 
{
	void *newPtr = malloc(sz);
	if(newPtr != NULL)
	{
		memcpy(newPtr, ptr, sz);
		free(ptr);
	}
	return newPtr;
}




