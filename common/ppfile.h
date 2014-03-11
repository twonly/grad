#ifndef __PPFILE_H__
#define __PPFILE_H__

#include <sys/stat.h>
#include <inttypes.h>

typedef struct _attr{
  int mode;
  int uid,gid;
  int atime,ctime,mtime;
  int link;
  int size;
} attr;

typedef struct _rep {
      int rep_ip;
      int visit_time;
      int history;
      int is_rep;
      struct _rep* next;
} rep;

#define CHUNKSIZE 4096

#define FILE_MAXCHUNKS 10000

typedef struct ppfile{
  char* path;
  char* name;
  attr a;

  int ref;

  int srcip; //the ip of the mds on which this file is located , primary

  //candidate list
  rep* rep_list;
  int rep_cnt;
  
  uint64_t* clist;
  int chunks;
  int alloced;

  struct ppfile* next;
  struct ppfile* child;
} ppfile;


ppfile* new_file(char* path,attr a);
void free_file(ppfile*);

int file_append_chunk(ppfile* f,uint64_t id);
int file_pop_chunk(ppfile* f,uint64_t* id);

char* parentdir(const char* path);

typedef struct heuristic {
    char *path;
    int mdsip;
    int visit;
    int history;
    //int timestamp;
    //int location;
} heuristic;

#endif
