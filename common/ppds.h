#ifndef __PPDS_H__
#define __PPDS_H__

typedef struct linklist{
  void* data;
  struct linklist* next;
} linklist;

typedef struct hashnode{
  char* key;
  void* data;
  
  struct hashnode* next;
} hashnode;

#if defined(__linux__)

char* strdup(const char*);

#endif

// djb2 hash function
static unsigned int strhash(char* s){
  unsigned int hash = 5381;
  int c;

  while ((c = *(s++)))
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

  return hash;
}

#endif
