#include "cache.h"

dir_cache* dc_qfront = NULL;
attr_cache* ac_qfront = NULL;
chunk_cache* cc_qfront = NULL;

#define push_queue(qfront,ptr) \
  if(qfront == NULL){\
    qfront = ptr;\
    ptr->prev = ptr->next = ptr;\
  } else {\
    ptr->prev = qfront->prev;\
    ptr->next = qfront;\
    qfront->prev->next = ptr;\
    qfront->prev = ptr;\
  }\


#define remove_from_queue(qfront,ptr)\
  if(qfront == ptr){\
    if(qfront->next == qfront){\
      qfront = NULL;\
    } else {\
      qfront->next->prev = qfront->prev;\
      qfront->prev->next = qfront->next;\
      qfront = qfront->next;\
    }\
  } else {\
    ptr->prev->next = ptr->next;\
    ptr->next->prev = ptr->prev;\
  }\


#define pop_queue(qfront)\
  if(qfront->next == qfront){\
    qfront = NULL;\
  } else {\
    qfront->next->prev = qfront->prev;\
    qfront->prev->next = qfront->next;\
    qfront = qfront->next;\
  }\

dir_cache* dir_cache_add(const char* path,char* entries[],int n){
  dir_cache* ret = malloc(sizeof(dir_cache));
  int i;

  ret->path = strdup(path);
  ret->add_time = time(NULL);

  ret->n = n;
  ret->entries = malloc(sizeof(char*)*(n+1));
  for(i=0;i<n;i++){
    ret->entries[i] = strdup(entries[i]);
  }

  push_queue(dc_qfront,ret)

  fprintf(stderr,"%s added to dc_cache\n",path);

  return ret;
}
attr_cache* attr_cache_add(const char* path,attr a){
  attr_cache* ret = malloc(sizeof(attr_cache));

  ret->path = strdup(path);
  ret->add_time = time(NULL);

  ret->a = a;

  push_queue(ac_qfront,ret)

  return ret;
}

chunk_cache* chunk_cache_add(const char* path,uint64_t* chunklist,int chunks,int mdsid){
  chunk_cache* ret = malloc(sizeof(chunk_cache));

  ret->path = strdup(path);
  ret->add_time = time(NULL);

  ret->chunks = chunks;
  ret->mdsid = mdsid;

  ret->chunklist = malloc(sizeof(uint64_t)*(chunks+1));
  memcpy(ret->chunklist,chunklist,sizeof(uint64_t)*chunks);
  

  push_queue(cc_qfront,ret)

  return ret;
}

int lookup_dir_cache(const char* path,dir_cache** c){
  while(dc_qfront){
    if(time(NULL) - dc_qfront->add_time <= CACHE_EXPIRE_TIME){
      break;
    }

    dir_cache* tmp = dc_qfront;
    pop_queue(dc_qfront)

    free_dir_cache(tmp);
  }

  dir_cache* dc = dc_qfront;
  if(!dc) return -1;

  while(dc->next != dc_qfront){
    if(!strcmp(dc->path,path)){
      *c = dc;

      remove_from_queue(dc_qfront,dc)
      push_queue(dc_qfront,dc)
      dc->add_time = time(NULL);
      return 0;
    }

    dc = dc->next;
  }

  if(!strcmp(dc->path,path)){
    *c = dc;

    remove_from_queue(dc_qfront,dc)
    push_queue(dc_qfront,dc)
    dc->add_time = time(NULL);

    return 0;
  }

  *c = NULL;
  return -1;
}

int lookup_attr_cache(const char* path,attr_cache** c){
  while(ac_qfront){
    if(time(NULL) - ac_qfront->add_time <= CACHE_EXPIRE_TIME){
      break;
    }

    attr_cache* tmp = ac_qfront;
    pop_queue(ac_qfront)

    free_attr_cache(tmp);
  }

  attr_cache* ac = ac_qfront;
  if(!ac) return -1;

  while(ac->next != ac_qfront){
    if(!strcmp(ac->path,path)){
      *c = ac;

      remove_from_queue(ac_qfront,ac)
      push_queue(ac_qfront,ac)
      ac->add_time = time(NULL);
      return 0;
    }
    ac = ac->next;
  }

  if(!strcmp(ac->path,path)){
    *c = ac;

    remove_from_queue(ac_qfront,ac)
    push_queue(ac_qfront,ac)
    ac->add_time = time(NULL);
    return 0;
  }

  *c = NULL;
  return -1;
}

int lookup_chunk_cache(const char* path,chunk_cache** c){
  while(cc_qfront){
    if(time(NULL) - cc_qfront->add_time <= CACHE_EXPIRE_TIME){
      break;
    }

    chunk_cache* tmp = cc_qfront;
    pop_queue(cc_qfront)

    free_chunk_cache(tmp);
  }

  chunk_cache* cc = cc_qfront;
  if(!cc) return -1;

  while(cc->next != cc_qfront){
    if(!strcmp(cc->path,path)){
      *c = cc;
  
      remove_from_queue(cc_qfront,cc)
      push_queue(cc_qfront,cc)
      cc->add_time = time(NULL);

      return 0;
    }

    cc = cc->next;
  }

  if(!strcmp(cc->path,path)){
    *c = cc;

    remove_from_queue(cc_qfront,cc)
    push_queue(cc_qfront,cc)
    cc->add_time = time(NULL);

    return 0;
  }

  *c = NULL;
  return -1;
}

void remove_dir_cache(dir_cache* c){
  remove_from_queue(dc_qfront,c);
}

void remove_attr_cache(attr_cache* c){
  remove_from_queue(ac_qfront,c);
}

void remove_chunk_cache(chunk_cache* c){
  remove_from_queue(cc_qfront,c);
}

void free_dir_cache(dir_cache* c){
  if(c){
    int i;

    free(c->path);
    for(i=0;i<c->n;i++){
      free(c->entries[i]);
    }
    free(c->entries);
    free(c);
  }
}

void free_attr_cache(attr_cache* c){
  if(c){
    free(c->path);
    free(c);
  }
}

void free_chunk_cache(chunk_cache* c){
  if(c){
    free(c->path);
    free(c->chunklist);
    free(c);
  }
}
