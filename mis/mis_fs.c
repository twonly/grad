#include "mis_fs.h"
#include <stdio.h>

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

int rename_all_child(ppfile* parent, ppfile* oldparent, ppfile* oldf, char* npath) {
    attr na = oldf->a;
    ppfile* nf = new_file(npath,na);
    //nf->srcip = eptr->peerip;
    add_file(nf); //add to hash list
    nf->next = parent->child;
    parent->child = nf; //add as nparent's child
    if(has_child(oldf)) {
        ppfile* child = oldf->child;
        while(child) {
            char * abspath = (char*)malloc(strlen(npath)+1+strlen(child->name));
            strcpy(abspath, npath);
            strcat(abspath, "/");
            strcat(abspath, child->name);
            rename_all_child(nf, oldf, child, abspath);
            fprintf(stderr, "absolute path of child is %s, free it\n", abspath);
            free(abspath); //something wrong here
            child = child->next;
        }
    }
    remove_child( oldparent, oldf );
}

int is_child(char* child, char* parent) { //for rename EINVAL
    return 0;
}

char* getparentdir(char* path, int len) { //do remember to free dir after using it
    char* dir;
    if(len > 1){
        dir = &path[len-1];
        while(dir >= path && *dir != '/') dir--;

        int dirlen = dir - path;
        if(dirlen==0) dirlen+=1;
        dir = strdup(path);
        dir[dirlen] = 0;
    } else {
        dir = strdup("/");
    }
    return dir;
}
