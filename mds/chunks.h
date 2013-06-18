#ifndef __CHUNKS_H__
#define __CHUNKS_H__

#include "ppfile.h"
#include "ppds.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct mdschunk{
  uint64_t chunkid;
  uint32_t csip;

  int occupy;
} mdschunk;

int chunks_init();
mdschunk* new_chunk(uint64_t chunkid,int ip,int occupy);
void free_chunk(mdschunk* c);

void add_chunk(mdschunk* c);
void remove_chunk(uint64_t chunkid);
mdschunk* lookup_chunk(uint64_t chunkid);

#endif
