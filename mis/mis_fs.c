#include "mis_fs.h"
#include <stdio.h>

int init_fs(){
  memset(tab,0,sizeof(tab));
  memset(nodetab,0,sizeof(nodetab));
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

static hashnode* ppnode_new(ppnode* n){
  hashnode* ret = (hashnode*)malloc(sizeof(hashnode));
  ret->key  = n->path;
  ret->data = (void*)n;
  ret->next = NULL;
  return ret;
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
static void ppnode_free(hashnode* n){
  free(n);
}

void add_ppnode(ppnode* pn){
  unsigned int k = strhash(pn->path) % HASHSIZE;

  hashnode *it = nodetab[k];
  while (it != NULL) {
    if(!strcmp(pn->path,it->key))  // path as key
        return;
    it = it->next;
  }

  hashnode* n = ppnode_new(pn); // f as data?
  n->next = nodetab[k];
  nodetab[k] = n; //add ppnode to hash table
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

void remove_ppnode(ppnode* pn){
  unsigned int k = strhash(pn->path) % HASHSIZE;
  hashnode* n = nodetab[k];
  hashnode* np = NULL;

  while(n){
    if(!strcmp(n->key,pn->path)){
      if(np == NULL){ //head of list
        nodetab[k] = n->next;
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

ppnode* lookup_ppnode(char* p){
  unsigned int k = strhash(p) % HASHSIZE;
  hashnode* n = nodetab[k];
  while(n){
    ppnode* pn = (ppnode*)n->data;
    if(!strcmp(pn->path,p)){
      return pn;
    }
    n = n->next;
  }
  return NULL;
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
void remove_childnode(ppnode* parent,ppnode* child){
  ppnode* it = parent->child;

  if(it != child){
    while(it && it->next != child){
      it = it->next;
    }

    if(it){
      it->next = child->next;
    } else {
        //return error? TODO
    }
  } else {
    parent->child = child->next;
  }

  remove_ppnode(child);
}

void remove_child(ppfile* pf,ppfile* f){
  ppfile* c = pf->child;

  if(c != f){
    while(c && c->next != f){
      c = c->next;
    }

    if(c){
      c->next = f->next;
    }
  } else {
    pf->child = f->next;
  }

  remove_file(f);
}

void update_visit(ppfile* f) {
    rep* r = f->rep_list;
    syslog(LOG_WARNING, "update %s replica info", f->path);
    while(r) {
        r->history = r->history/2 + r->visit_time;
        r->visit_time = 0;
        syslog(LOG_WARNING, "after update %s: ip:%X, history:%d, visit_time:%d", f->path, r->rep_ip, r->history, r->visit_time);
        r = r->next;
    }
    return;
}

void update_visit_all(void) {
    syslog(LOG_WARNING, "update all replica info");
  int i;
  for(i=0; i<HASHSIZE; ++i) { //HASHSIZE/2?
      hashnode* n = tab[i];
      while(n) {
          ppfile* f = (ppfile*)(n->data);
          update_visit(f);      
          n = n->next;
      }
  }
  return;
}

#define MAX_QUEUE_SIZE 1000

//typedef struct queue_{
//      ppfile* f;
//  struct queue_* next;
//} queue;
//
//static void enq(ppfile* f,queue** front,queue** rear){
//  queue* n = (queue*)malloc(sizeof(queue));
//  n->f = f;
//  if(*rear == NULL){
//    *front = *rear = n;
//  } else {
//    (*rear)->next = n;
//    *rear = n;
//  }
//}
//
//static ppfile* deq(queue** front,queue** rear){
//  if(*front == NULL){
//    return NULL;
//  }
//
//  ppfile* ret = (*front)->f;
//  queue* n = *front;
//
//  if(*front == *rear){
//    *front = *rear = NULL;
//  } else {
//    *front = (*front)->next;
//  }
//
//  free(n);
//  return ret;
//}
//
//static int is_empty(const queue* front,const queue* rear){
//  if(front == NULL && rear == NULL){
//    return 1;
//  }
//
//  return 0;
//}
//
//static void pickle_attr(FILE* fp,attr a){
//  fprintf(fp,"%d\n",a.mode);
//  fprintf(fp,"%d\t%d\n",a.uid,a.gid);
//  fprintf(fp,"%d\t%d\t%d\n",a.atime,a.ctime,a.mtime);
//  fprintf(fp,"%d\t%d\n",a.link,a.size);
//}
//
//static void unpickle_attr(FILE* fp,attr* a){
//  fscanf(fp,"%d",&a->mode);
//  fscanf(fp,"%d%d",&a->uid,&a->gid);
//  fscanf(fp,"%d%d%d",&a->atime,&a->ctime,&a->mtime);
//  fscanf(fp,"%d%d",&a->link,&a->size);
//}
//
//
//void pickle(char* path){
//  int i;
//  FILE* fp = fopen(path,"w");
//  if(!fp){
//    fprintf(stderr,"failed to pickle %s, cannot open file!\n",path);
//    return;
//  }
//
//  queue *front,*rear;
//
//  front = rear = NULL;
//  ppfile* root = lookup_file("/");
//  enq(root,&front,&rear);
//  while(!is_empty(front,rear)){
//      ppfile* f = deq(&front,&rear);
//
//      fprintf(fp,"%s\t%x\n",f->path,f->srcip);
//      pickle_attr(fp,f->a);
//
//      if(f->next){
//        enq(f->next,&front,&rear);
//      }
//
//      if(f->child){
//        enq(f->child,&front,&rear);
//      }
//  }
//
//  fclose(fp);
//}
//
//void unpickle(char* path){
//  int srcip;
//  FILE* fp = fopen(path,"r");
//  attr a;
//  char buf[200];
//
//  if(!fp){
//    fprintf(stderr,"failed to unpickle %s,cannot open file!\n",path);
//    return;
//  }
//
//  while(fscanf(fp,"%s%x",buf,&srcip) != EOF){
//    unpickle_attr(fp,&a);
//
//    ppfile* f = new_file(buf,a);
//    f->srcip = srcip;
//    add_file(f);
//
//    if(strcmp(buf,"/")){
//      char* base = parentdir(buf);
//      ppfile* pf = lookup_file(base);
//      if(!pf){
//        fprintf(stderr,"unpickle error, didn't find parent dir %s for %s\n",base,buf);
//      } else {
//        f->next = pf->child;
//        pf->child = f;
//      }
//
//      free(base);
//    }
//  }
//
//  fclose(fp);
//}
//
