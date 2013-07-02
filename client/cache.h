#ifndef __PPFS_CACHE_H__
#define __PPFS_CACHE_H__

#include "ppfile.h"
#include "ppds.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define CACHE_EXPIRE_TIME 10 // in seconds

typedef struct dir_cache{
  char* path;
  time_t add_time;

  char** entries;
  int n;

  struct dir_cache *next,*prev;
} dir_cache;

typedef struct attr_cache{
  char* path;
  time_t add_time;

  attr a;

  struct attr_cache *next,*prev;
} attr_cache;

typedef struct chunk_cache{
  char* path;
  time_t add_time;

  uint64_t* chunklist;
  int chunks;
  uint32_t mdsid;

  struct chunk_cache *next,*prev;
} chunk_cache;

dir_cache* dir_cache_add(const char* path,char* entries[],int n);
attr_cache* attr_cache_add(const char* path,attr a);
chunk_cache* chunk_cache_add(const char* path,uint64_t* chunklist,int chunks,int mdsid);

int lookup_dir_cache(const char* path,dir_cache** c);
int lookup_attr_cache(const char* path,attr_cache** c);
int lookup_chunk_cache(const char* path,chunk_cache** c);

void remove_dir_cache(dir_cache* c);
void remove_attr_cache(attr_cache* c);
void remove_chunk_cache(chunk_cache* c);

void free_dir_cache(dir_cache* c);
void free_attr_cache(attr_cache* c);
void free_chunk_cache(chunk_cache* c);

#endif
