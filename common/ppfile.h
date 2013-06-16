#ifndef __PPFILE_H__
#define __PPFILE_H__

#include <sys/stat.h>

typedef struct _attr{
  int mode;
  int uid,gid;
  int atime,ctime,mtime;
  int link;
  int size;
} attr;

typedef struct ppfile{
  char* path;
  attr s;

  int ref;

  struct ppfile* next;
  struct ppfile* child;
} ppfile;

#endif
