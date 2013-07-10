#ifndef __PPFILE_H__
#define __PPFILE_H__

#include <sys/stat.h>
#include <inttypes.h>

typedef struct _attr{
  int mode;
  int uid,gid;
  int atime,ctime,mtime;
  int link;
  int size;
} attr;

#define CHUNKSIZE 4096

#define FILE_MAXCHUNKS 10000

typedef struct ppfile{
  char* path;
  char* name;
  attr a;

  int ref;

  int srcip; //where this file is located

  uint64_t* clist;
  int chunks;
  int alloced;

  struct ppfile* next;
  struct ppfile* child;
} ppfile;


ppfile* new_file(char* path,attr a);
void free_file(ppfile*);

int file_append_chunk(ppfile* f,uint64_t id);
int file_pop_chunk(ppfile* f,uint64_t* id);

char* parentdir(const char* path);

#endif
