#include "chunks.h"

#define CHUNKTABSIZE 4651

typedef struct cshash{
  uint64_t key;
  cschunk* data;
  struct cshash* next;
} cshash;

static cshash* tab[CHUNKTABSIZE];

static int space,availspace,chunks;

int hash(uint64_t id){
  return (id & 0xFFFF) % CHUNKTABSIZE;
}

void get_chunk_info(int* _space,int* _availspace,int* _chunks){
  *_space = space;
  *_availspace = availspace;
  *_chunks = chunks;
}

int chunks_init(){
  memset(tab,0,sizeof(tab));
  chunklist = NULL;
  space = CHUNKTABSIZE*10*CHUNKSIZE;//just for demo
  availspace = space;
  chunks = 0;

  return 0;
}

cschunk* new_chunk(uint64_t chunkid){
  cschunk* ret = (cschunk*)malloc(sizeof(cschunk));
  ret->chunkid = chunkid;
  ret->buf = (uint8_t*)malloc(CHUNKSIZE);
  ret->occupy = 0;

  linklist* n = (linklist*)malloc(sizeof(linklist));
  n->data = ret;
  n->next = chunklist;
  n->prev = NULL;
  if(chunklist)
    chunklist->prev = n;
  chunklist = n;

  ret->reserved = n;

  chunks++;
  availspace -= CHUNKSIZE;

  return ret;
}

void free_chunk(cschunk* c){
  if(c){
    linklist* n = (linklist*)c->reserved;
    if(n->prev == NULL){
      chunklist = n->next;
      if(chunklist)
        chunklist->prev = NULL;
    } else {
      n->prev->next = n->next;
      if(n->next)
        n->next->prev = n->prev;
    }
    free(n);

    free(c->buf);
    free(c);

    availspace += CHUNKSIZE;
  }
}

void add_chunk(cschunk* c){
  int k = hash(c->chunkid);
  cshash* n = (cshash*)malloc(sizeof(cshash));
  n->data = c;
  n->key = c->chunkid;
  n->next = NULL;

  if(tab[k] == NULL){
    tab[k] = n;
  } else {
    n->next = tab[k];
    tab[k] = n;
  }
}

void remove_chunk(uint64_t chunkid){
  int k = hash(chunkid);
  cshash *n,*np;

  if(!tab[k]) return;

  n = tab[k];
  np = NULL;
  while(n){
    if(n->key == chunkid) break;

    np = n;
    n = n->next;
  }

  if(n == NULL){
    return;
  }

  if(np == NULL){
    tab[k] = n->next;
  } else {
    np->next = n->next;
  }

  free(n);
}

cschunk* lookup_chunk(uint64_t chunkid){
  int k = hash(chunkid);
  cshash *n;

  n = tab[k];
  while(n){
    if(n->key == chunkid) return n->data;

    n = n->next;
  }

  return NULL;
}
