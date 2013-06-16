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
