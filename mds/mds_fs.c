#include "mds_fs.h"
#include <stdio.h>
#include <syslog.h>

static hashnode* tab[HASHSIZE];

int init_fs(){
  memset(tab,0,sizeof(tab));

  return 0;
}

void term_fs(){
  //clean up?
}

static hashnode* node_new(ppfile* f){
  hashnode* ret = (hashnode*)malloc(sizeof(hashnode));
  ret->key = f->path;
  ret->data = (void*)f;
  ret->next = NULL;

  return ret;
}

static void node_free(hashnode* n){
  free(n);
}

void add_file(ppfile* f){
  syslog(LOG_WARNING, "add file %s", f->path);
  unsigned int k = strhash(f->path) % HASHSIZE;
    fprintf(stderr,"add file: %s, index: %u",f->path, k);
  hashnode *it = tab[k];

  while( it!=NULL ) {
    if(!strcmp(f->path,it->key)) {
      fprintf(stderr,"compare file: %s",it->key);
      return;
    }
    it = it->next;
  }
  hashnode* n = node_new(f);
  n->next = tab[k];
  tab[k] = n;
}

void remove_file(ppfile* f){
  syslog(LOG_WARNING, "remove file %s", f->path);
  unsigned int k = strhash(f->path) % HASHSIZE;
    fprintf(stderr,"remove file: %s, index: %u\n",f->path, k);
  hashnode* n = tab[k];
  hashnode* np = NULL;

  while(n){
    fprintf(stderr,"compare file: %s\n",n->key);
    if(!strcmp(n->key,f->path)){
      if(np == NULL){ //head of list
        tab[k] = n->next;
        free(n); //should return right now?
      } else {
        np->next = n->next;
        free(n);
      }
    }

    np = n;
    n = n->next;
  }
}

ppfile* lookup_file(char* p){
  unsigned int k = strhash(p) % HASHSIZE;
    fprintf(stderr,"lookup file: %s, index: %u\n", p, k);
  hashnode* n = tab[k];
  syslog(LOG_WARNING, "lookup file %s", p);
  while(n){
    ppfile* f = (ppfile*)(n->data);
    syslog(LOG_WARNING, "compare file %s", f->path);
    if(!strcmp(f->path,p)){
      return f;
    }

    n = n->next;
  }

  return NULL;
}
