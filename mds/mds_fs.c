#include "mds_fs.h"
#include <stdio.h>
#include <syslog.h>
#include <sys/stat.h>
#include "main.h"


int init_fs(){
  memset(tab,0,sizeof(tab));
  memset(reptab,0,sizeof(reptab));
  struct stat st;
  if(stat(DUMP_FILE,&st) != -1){
    //unpickle(DUMP_FILE);
  }

  return 0;
}

void term_fs(){
  struct stat st;
  char path[100];
  uint32_t t = main_time();
  if(stat(DUMP_FILE,&st) != -1){
    sprintf(path,"%s.%d.old.dump",DUMP_FILE,t);
    if(rename(DUMP_FILE,path) != 0){
      fprintf(stderr,"failed to back up old fs record\n");
    }
  }

  //pickle(DUMP_FILE);
}

static hashnode* repnode_new(pprep* f){
  hashnode* ret = (hashnode*)malloc(sizeof(hashnode));
  ret->key = f->path;
  ret->data = (void*)f;
  ret->next = NULL;
  return ret;
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

void add_rep(pprep* f){
  //syslog(LOG_WARNING, "add file %s", f->path);
  unsigned int k = strhash(f->path) % HASHSIZE;
    fprintf(stderr,"add rep: %s, index: %u\n",f->path, k);
  hashnode *it = reptab[k];

  while( it!=NULL ) {
    if(!strcmp(f->path,it->key)) {
      fprintf(stderr,"compare file: %s",it->key);
      return;
    }
    it = it->next;
  }
  hashnode* n = repnode_new(f);
  n->next = reptab[k];
  reptab[k] = n;
}
void add_file(ppfile* f){
  //syslog(LOG_WARNING, "add file %s", f->path);
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

void remove_rep(pprep* f){
  unsigned int k = strhash(f->path) % HASHSIZE;
    fprintf(stderr,"remove rep: %s, index: %u\n",f->path, k);
  hashnode* n = reptab[k];
  hashnode* np = NULL;

  while(n){
    //fprintf(stderr,"compare file: %s\n",n->key);
    if(!strcmp(n->key,f->path)){
      if(np == NULL){ //head of list
        reptab[k] = n->next;
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
void remove_file(ppfile* f){
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

pprep* lookup_rep(char* p){
  unsigned int k = strhash(p) % HASHSIZE;
    fprintf(stderr,"lookup rep: %s, index: %u\n", p, k);
  hashnode* n = reptab[k];
  while(n){
    pprep* f = (pprep*)(n->data);
    if(!strcmp(f->path,p)){
      return f;
    }
    n = n->next;
  }

  return NULL;
}
ppfile* lookup_file(char* p){
  unsigned int k = strhash(p) % HASHSIZE;
    fprintf(stderr,"lookup file: %s, index: %u\n", p, k);
  hashnode* n = tab[k];
  //syslog(LOG_WARNING, "lookup file %s", p);
  while(n){
    ppfile* f = (ppfile*)(n->data);
    //syslog(LOG_WARNING, "compare file %s", f->path);
    if(!strcmp(f->path,p)){
      return f;
    }

    n = n->next;
  }

  return NULL;
}

//@TODO: The two functions below does not support chunkids yet

static void pickle_attr(FILE* fp,attr a){
  fprintf(fp,"%d\n",a.mode);
  fprintf(fp,"%d\t%d\n",a.uid,a.gid);
  fprintf(fp,"%d\t%d\t%d\n",a.atime,a.ctime,a.mtime);
  fprintf(fp,"%d\t%d\n",a.link,a.size);
}

static void unpickle_attr(FILE* fp,attr* a){
  fscanf(fp,"%d",&a->mode);
  fscanf(fp,"%d%d",&a->uid,&a->gid);
  fscanf(fp,"%d%d%d",&a->atime,&a->ctime,&a->mtime);
  fscanf(fp,"%d%d",&a->link,&a->size);
}

void pickle(char* path){
  int i;
  FILE* fp = fopen(path,"w");
  if(!fp){
    fprintf(stderr,"failed to pickle %s, cannot open file!\n",path);
    return;
  }

  for(i=0;i<HASHSIZE;i++){
    hashnode* n = tab[i];
    while(n){
      ppfile* f = (ppfile*)(n->data);
      fprintf(fp,"%s\n",f->path);
      pickle_attr(fp,f->a);

      n = n->next;
    }
  }

  fclose(fp);
}

void unpickle(char* path){
  int i;
  FILE* fp = fopen(path,"r");
  attr a;
  char buf[200];

  if(!fp){
    fprintf(stderr,"failed to unpickle %s,cannot open file!\n",path);
    return;
  }

  while(fscanf(fp,"%s",buf) != EOF){
    unpickle_attr(fp,&a);

    ppfile* f = new_file(buf,a);
    add_file(f);
  }

  fclose(fp);
}

