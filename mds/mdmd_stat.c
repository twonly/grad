#include "mdmd_stat.h"

char* stat_path = ".";

typedef struct mdmd_stat_entry{
  int key;
  char* desc;

  int count;

  struct mdmd_stat_entry* next;
} mdmd_stat_entry;

mdmd_stat_entry* mdmdstathead = NULL;

int mdmd_stat_init(void){
	main_destructregister(mdmd_stat_term);
  main_timeregister(TIMEMODE_RUN_LATE,LOG_PERIOD,0,mdmd_stat_dump);
}

void mdmd_stat_term(void){
  mdmd_stat_dump();

  mdmd_stat_entry* eptr,*neptr;
  eptr = mdmdstathead;
  neptr = NULL;
  while(eptr){
    neptr = eptr->next;

    free(eptr->desc);
    free(eptr);

    eptr = neptr;
  }
}

void mdmd_stat_add_entry(int key,char* desc,int c0){
  mdmd_stat_entry* eptr = mdmdstathead;
  while(eptr){
    if(eptr->key == key){
      return;
    }

    eptr = eptr->next;
  }

  eptr = malloc(sizeof(mdmd_stat_entry));
  eptr->desc = strdup(desc);
  eptr->key = key;
  eptr->count = c0;
  eptr->next = mdmdstathead;
  mdmdstathead = eptr;
}

void mdmd_stat_count(int key){
  mdmd_stat_entry* eptr = mdmdstathead;
  while(eptr){
    if(eptr->key == key){
      eptr->count++;

      return;
    }

    eptr = eptr->next;
  }
}

void mdmd_stat_countm(int key,int c){
  mdmd_stat_entry* eptr = mdmdstathead;
  while(eptr){
    if(eptr->key == key){
      eptr->count += c;

      return;
    }

    eptr = eptr->next;
  }
}

void mdmd_stat_dump(){
  char path[100];
  int t = main_time();
  static FILE* fp = NULL;

  if(fp == NULL){
    sprintf(path,"%s/%d_mdmd_stat.log",stat_path,t);

    fp = fopen(path,"w");
    if(!fp){
      fprintf(stderr,"failed to create dump file in path:%s\n",path);

      sprintf(path,"%d_mdmd_stat.log",t);

      fp = fopen(path,"w");
    }

    if(!fp){
      fprintf(stderr,"failed to create dump file in path:%s\n",path);
    }

    if(fp){
      fprintf(stderr,"stat_file_path:%s\n",path);
    }
  }

  mdmd_stat_entry* eptr = mdmdstathead;
  fprintf(stderr,"(%d):{\n",t);
  if(fp)
    fprintf(fp,"(%d):{\n",t);

  while(eptr){
    fprintf(stderr,"(%s)%d  %d\n",eptr->desc,eptr->key,eptr->count);
    if(fp)
      fprintf(fp,"(%s)%d  %d\n",eptr->desc,eptr->key,eptr->count);

    eptr = eptr->next;
  }

  fprintf(stderr,"}\n");
  if(fp)
    fprintf(fp,"}\n\n");

  /*if(fp)*/
    /*fclose(fp);*/
  if(fp)
    fflush(fp);
}
