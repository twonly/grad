#ifndef __CLIENT_H__
#define __CLIENT_H__

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fuse.h>
#include <unistd.h>
#include "sockets.h"
#include "sockets.c"
#include "ppcomm.h"
#include "ppcomm.c"
#include "ppfile.h"
#include "datapack.h"


typedef int(* fuse_fill_dir_t)(void *buf, const char *name, const struct stat *stbuf, off_t off);
void*   ppfs_fsinit(struct fuse_conn_info *conn);
int     ppfs_getattr(const char*, struct stat*); //always called before open
int     ppfs_mkdir(const char*, mode_t);
int     ppfs_link(const char*, const char*);
int     ppfs_opendir(const char*, struct fuse_file_info*);
int     ppfs_open(const char*, struct fuse_file_info*);
int     ppfs_getxattr(const char*, const char*, char*, size_t);
int     ppfs_listxattr(const char*, char*, size_t);
int     ppfs_mknod(const char *path, mode_t mt, dev_t dt);
int 	ppfs_access (const char *, int);
int 	ppfs_chmod (const char *, mode_t);
int 	ppfs_chown (const char *, uid_t, gid_t);
int 	ppfs_create (const char *, mode_t, struct fuse_file_info *);
int 	ppfs_flush (const char *, struct fuse_file_info *);
int 	ppfs_fsync (const char *, int, struct fuse_file_info *);
int 	ppfs_read (const char *, char *, size_t, off_t, struct fuse_file_info *);
int 	ppfs_readdir (const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int 	ppfs_readlink (const char *, char *, size_t);
int 	ppfs_release (const char *, struct fuse_file_info *);
int 	ppfs_releasedir (const char *, struct fuse_file_info *);
int 	ppfs_removexattr (const char *, const char *);
int 	ppfs_rename (const char *, const char *);
int 	ppfs_rmdir (const char *);
int 	ppfs_setxattr (const char *, const char *, const char *, size_t, int);
int     ppfs_statfs (const char *, struct statvfs *);
int 	ppfs_symlink (const char *, const char *);
int 	ppfs_unlink (const char *);
int 	ppfs_write (const char *, const char *, size_t, off_t, struct fuse_file_info *);

#endif
