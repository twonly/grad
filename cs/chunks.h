#ifndef __CHUNKS_H__
#define __CHUNKS_H__

#include "ppfile.h"
#include "ppds.h"
#include <stdlib.h>
#include <string.h>

typedef struct cschunk{
  uint64_t chunkid;
  uint8_t* buf;
  int occupy;

  void* reserved;
} cschunk;

linklist* chunklist;

void get_chunk_info(int* space,int* availspace,int* chunks);

int chunks_init();
cschunk* new_chunk(uint64_t chunkid);
void free_chunk(cschunk* c);

void add_chunk(cschunk* c);
void remove_chunk(uint64_t chunkid);
cschunk* lookup_chunk(uint64_t chunkid);

#endif
