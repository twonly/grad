#ifndef __PPFILE_H__
#define __PPFILE_H__


typedef struct ppfile{
  int mode;

  char* path;

  int uid,gid;
  int atime,ctime,mtime;

  int link;
  int ref;

  union{
    struct {
      linklist* files;
    } dentry;

    struct {
      int size;
    } regfile;
  } s;

} ppfile;

#endif
