#include "chunks.h"

#define CHUNKTABSIZE 11173

typedef struct mdshash{
  uint64_t key;
  mdschunk* data;

  struct mdshash* next;
} mdshash;

static mdshash* tab[CHUNKTABSIZE];

int hash(uint64_t id){
  return (id & 0xFFFF) % CHUNKTABSIZE;
}

int chunks_init(){
  memset(tab,0,sizeof(tab));

  return 0;
}

mdschunk* new_chunk(uint64_t chunkid,int ip,int occupy){
  mdschunk* ret = (mdschunk*)malloc(sizeof(mdschunk));

  ret->chunkid = chunkid;
  ret->occupy = occupy;
  ret->csip = ip;

  return ret;
}

void free_chunk(mdschunk* c){
  if(c){
    free(c);
  }
}

void add_chunk(mdschunk* c){
  fprintf(stderr,"add chunk:%lld\n",c->chunkid);

  int k = hash(c->chunkid);
  mdshash* n = (mdshash*)malloc(sizeof(mdshash));
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
  mdshash *n,*np;

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

mdschunk* lookup_chunk(uint64_t chunkid){
  int k = hash(chunkid);
  mdshash *n;

  n = tab[k];
  while(n){
    if(n->key == chunkid) return n->data;

    n = n->next;
  }

  return NULL;
}
