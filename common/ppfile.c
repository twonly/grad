#include "ppfile.h"
#include "ppds.h"
#include <stdlib.h>
#include <string.h>

char* getbasename(char* path){
  int pos = strlen(path) - 1;
  while(pos >= 0 && path[pos] != '/')
    pos--;

  return (path + pos + 1);
}

ppfile* new_file(char* path,attr a){
  ppfile* ret = (ppfile*)malloc(sizeof(ppfile));
  ret->path = strdup(path);
  ret->srcip = 0;

  ret->clist = NULL;
  ret->chunks = 0;
  ret->alloced = 0;

  if(!strcmp(path,"/")){
    ret->name = ret->path;
  } else {
    ret->name = getbasename(ret->path);
  }

  ret->a = a;
  ret->next = ret->child = NULL;
  return ret;
}

void free_file(ppfile* f){
  if(f){
    free(f->path);
    free(f);
  }
}

int file_append_chunk(ppfile* f,uint64_t id){//should set a limit
  if(f->clist == NULL){
    f->alloced = 5;
    f->clist = (uint64_t*)malloc(sizeof(uint64_t)*5);
    f->clist[f->chunks++] = id;
  } else if(f->alloced - f->chunks > 0){
    f->clist[f->chunks++] = id;
  } else {
    f->alloced <<= 1;
    if(f->alloced >= FILE_MAXCHUNKS){
      f->alloced >>= 1;

      return -1;
    }

    f->clist = (uint64_t*)realloc(f->clist,sizeof(uint64_t)*f->alloced);
    f->clist[f->chunks++] = id;
  }

  f->a.size += CHUNKSIZE;

  return 0;
}

int file_pop_chunk(ppfile* f,uint64_t* id){
  if(f->clist == NULL || f->chunks == 0){
    return -1;
  }

  f->chunks--;
  *id = f->clist[f->chunks];
  f->a.size -= CHUNKSIZE;

  return 0;
}
