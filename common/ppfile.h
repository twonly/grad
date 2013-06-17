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

typedef struct chunk{
  uint64_t chunkid;
  uint32_t servip;

  int blocks;
  int blocksize;
} chunk;

typedef struct ppfile{
  char* path;
  char* name;
  attr a;

  int ref;

  int srcip; //where this file is located

  chunk* clist;

  struct ppfile* next;
  struct ppfile* child;
} ppfile;


ppfile* new_file(char* path,attr a);
void free_file(ppfile*);

#endif
