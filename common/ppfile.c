#include "ppfile.h"
#include "ppds.h"
#include <stdlib.h>
#include <string.h>

char* getbasename(char* path){
  int pos = strlen(path) - 1;
  if(pos==0) return path; //"/"
  while(pos >= 0 && path[pos] != '/')
    pos--;

  return (path + pos + 1);
}

pprep* new_pprep(char* path, attr a) {
    pprep* ret = (pprep*)malloc(sizeof(pprep));
    ret->path = strdup(path);
    ret->primaryip = 0;
    ret->a = a;
    ret->age = 0;
    ret->visit_time = 0;
    ret->history = 0;
    return ret;
}

void free_pprep(pprep* r) {
    if(r) {
        free(r->path);
        free(r);
    }
}
ppnode* new_ppnode(char* path) {
    ppnode* ret = (ppnode*)malloc(sizeof(ppnode));
    ret->path = strdup(path);
    ret->primaryip = 0;
    ret->repip_list = NULL;
    //ret->isdir = 0;
    ret->a = NULL;
    //ret->a = 0;
    //if(!strcmp(path,"/")){
    //    //ret->name = ret->path;
    //    //ret->isdir = 1;
    //} else {
    //    ret->name = getbasename(ret->path);
    //}
    ret->next = ret->child = NULL;
    return ret;
}

void free_ppnode(ppnode* n) {
    if(n) {
        free(n->path);
        free(n);
    }
}

ppfile* new_file(char* path,attr a){
  ppfile* ret = (ppfile*)malloc(sizeof(ppfile));
  ret->path = strdup(path);
  ret->srcip = 0;

  ret->rep_list = NULL;
  ret->rep_cnt = 0;

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

  //f->a.size += CHUNKSIZE;

  return 0;
}

int file_pop_chunk(ppfile* f,uint64_t* id){
  if(f->clist == NULL || f->chunks == 0){
    return -1;
  }

  f->chunks--;
  if(id)
    *id = f->clist[f->chunks];

  if(f->a.size > f->chunks * CHUNKSIZE)
    f->a.size = f->chunks * CHUNKSIZE;

  return 0;
}

char* parentdir(const char* path){
  int len = strlen(path);
  char* ret = malloc(len+10);
  strcpy(ret,path);

  int i = len-1;
  while(i >= 0 && ret[i] != '/') i--;
  if(i <= 0){
    ret[0] = '/';
    ret[1] = 0;
  } else {
    ret[i] = 0;
  }

  return ret;
}

