#include "mis_fs.h"

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
  ret->key  = f->path;
  ret->data = (void*)f;
  ret->next = NULL;

  return ret;
}

static void node_free(hashnode* n){
  free(n);
}

void add_file(ppfile* f){
  unsigned int k = strhash(f->path) % HASHSIZE;

  hashnode *it = tab[k];
  while (it != NULL) {
    if(!strcmp(f->path,it->key))  //??? path as key
        return;
    it = it->next;
  }

  hashnode* n = node_new(f); // f as data?
  n->next = tab[k];
  tab[k] = n;
}

void remove_file(ppfile* f){
  unsigned int k = strhash(f->path) % HASHSIZE;
  hashnode* n = tab[k];
  hashnode* np = NULL;

  while(n){
    if(!strcmp(n->key,f->path)){
      if(np == NULL){ //head of list
        tab[k] = n->next;
        free(n);
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
  hashnode* n = tab[k];
  while(n){
    ppfile* f = (ppfile*)n->data;
    if(!strcmp(f->path,p)){
      return f;
    }

    n = n->next;
  }

  return NULL;
}

int remove_child(ppfile* parent, ppfile* child) {
    ppfile *it = parent->child;
    ppfile *pre = NULL; 
    while( it ) {
        if(!strcmp(it->path, child->path)) {
            if(!pre) { //first node
                parent->child = it->next;
            } else {
                pre->next = it->next;
            }
            remove_file(it);
            free_file(it);
            return 0;
        } else {
            pre = it;
            it = it->next;
        }
    }
    return -1;
}

void remove_all_child(ppfile* parent) { //remove a dir recursively
    ppfile* child = parent->child;
    while( child ) {
        remove_all_child(child);
        remove_child(parent, child);
        child = parent->child;
    }
}

int has_child(ppfile* parent) {
    return (parent->child?1:0);
}
