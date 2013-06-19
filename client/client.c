#define FUSE_USE_VERSION 26
#include "client.h"

void sendpacket(int sockfd,ppacket* p){
  int i;
  int len = 0;

  while(p->bytesleft){
    i = write(sockfd,p->startptr,p->bytesleft);
    if(i < 0){
      if(i != -EAGAIN){
        fprintf(stderr,"write error:%d\n",i);
        exit(1);
      }
      continue;
    }
    if(i==0){
      fprintf(stderr,"Connection closed\n");
      exit(1);
    }

    len += i;
    p->startptr += i;
    p->bytesleft -= i;
  }

  printf("%d bytes sent\n",len);
}

ppacket* receivepacket(int sockfd){
  char headbuf[20];
  uint8_t* startptr;
  const uint8_t* ptr;
  int i,hleft;
  ppacket* p;
  syslog(LOG_WARNING, "in receivepacket");

  hleft = 8;
  startptr = headbuf;
  while(hleft){
    i = read(sockfd,startptr,hleft);
    if(i < 0){
      if(i != -EAGAIN){
        fprintf(stderr,"read error:%d\n",i);
        exit(1);
      }
      continue;
    }
    if(i==0){
      fprintf(stderr,"Connection closed\n");
      exit(1);
    }

    hleft -= i;
    startptr += i;
  }

  int size,cmd;
  ptr = headbuf;
  size = get32bit(&ptr);
  cmd = get32bit(&ptr);

  printf("got packet:size=%d,cmd=%X\n",size,cmd);
  p = createpacket_r(size,cmd,1);
  
  while(p->bytesleft){
    i = read(sockfd,p->startptr,p->bytesleft);
    if(i < 0){
      if(i != -EAGAIN){
        fprintf(stderr,"read error:%d\n",i);
        exit(1);
      }
      continue;
    }
    if(i==0){
      fprintf(stderr,"Connection closed\n");
      exit(1);
    }

    p->bytesleft -= i;
    p->startptr += i;
  }

  p->startptr = p->buf;

  return p;
}

void print_attr(const attr* a){
  if(a->mode & S_IFDIR){
    syslog(LOG_WARNING,"\tdirectory\n");
  } else if(a->mode & S_IFREG){
    syslog(LOG_WARNING,"\tregular file\n");
  }


  syslog(LOG_WARNING,"\tperm:%d%d%d\n",a->mode / 0100 & 7,
                           a->mode / 0010 & 7,
                           a->mode & 7);

  syslog(LOG_WARNING,"\tuid=%d,gid=%d",a->uid,a->gid);
  syslog(LOG_WARNING,"\tatime=%d,ctime=%d,mtime=%d\n",a->atime,a->ctime,a->mtime);
  syslog(LOG_WARNING,"\tlink=%d\n",a->link);
  syslog(LOG_WARNING,"\tsize=%d\n",a->size);
}

static struct fuse_operations ppfs_oper = {
	.init       = ppfs_fsinit, //connect to mds
	.statfs		= ppfs_statfs,
	.getattr	= ppfs_getattr,
	.mknod		= ppfs_mknod,
	.unlink		= ppfs_unlink,
	.mkdir		= ppfs_mkdir,
	.rmdir		= ppfs_rmdir,
	.symlink	= ppfs_symlink,
	.readlink	= ppfs_readlink,
	.rename		= ppfs_rename,
	.link		= ppfs_link,
	.opendir	= ppfs_opendir,
	.readdir	= ppfs_readdir,
	.releasedir	= ppfs_releasedir,
	.create		= ppfs_create,
	.open		= ppfs_open, //called before read
	.release	= ppfs_release,
	.flush		= ppfs_flush,
	.fsync		= ppfs_fsync,
	.read		= ppfs_read,
	.write		= ppfs_write,
	.access		= ppfs_access,
};

int fd;
char *ip = "127.0.0.1";
char *port = "8125";
char *hello_path = "/hello";
char *hello_str = "hello world";

void* ppfs_fsinit( struct fuse_conn_info* conn ) { //connect to MDS
    syslog(LOG_WARNING, "ppfs_fsinit");
    if((fd = tcpsocket())<0) {
        fprintf(stderr, "can't create socket");
    } else {
        syslog(LOG_WARNING, "fd:%d", fd);
    }

    //tcpnodelay(fd);
    /*if(tcpstrbind(fd, ip, port)<0) {
        syslog(LOG_WARNING, "can't bind to (%s:%s)", ip, port);
        return;
    } else {
        syslog(LOG_WARNING, "after bind");
    }*/
    if(tcpstrconnect(fd, ip, port)<0) {
        syslog(LOG_WARNING, "can't connect to MDS (%s:%s)", ip, port);
        fd = -1;
        return;
    }
    syslog(LOG_WARNING, "ppfs_init create socket: %u", fd);
}
int ppfs_getattr(const char* path, struct stat* stbuf){
        syslog(LOG_WARNING, "ppfs_getattr path : %s", path);
        ppacket *s = createpacket_s(4+strlen(path), CLTOMD_GETATTR,1);
        syslog(LOG_WARNING, "createpacket_s packet size:%u, cmd:%u", s->size, s->cmd); //5,4096
        uint8_t* ptr = s->startptr+8;
        put32bit(&ptr, strlen(path));
        memcpy(ptr, path, strlen(path));
        ptr+=strlen(path);
        sendpacket(fd, s);
        syslog(LOG_WARNING, "sendpacket");
        //free(s->buf);
        free(s);
        s = receivepacket(fd);
        syslog(LOG_WARNING, "receivepacket");
        const uint8_t* ptr2 = s->startptr;
        int status = get32bit(&ptr2);
        syslog(LOG_WARNING,"status:%d\n",status);
        if(status == 0){
            syslog(LOG_WARNING,"getattr success");
            print_attr((const attr*)ptr2);
        } else {
            if(status == -ENOENT){
                syslog(LOG_WARNING,"\tENOENT\n");
            }
            if(status == -ENOTDIR){
                syslog(LOG_WARNING,"\tENOTDIR\n");
            }
        }
        //free(s->buf);
        free(s);
        int res = 0;
      memset(stbuf, 0, sizeof(struct stat));
      if(strcmp(path, "/") == 0) {
          stbuf->st_mode = S_IFDIR | 0755;
          stbuf->st_nlink = 2;
      }
      else if(strcmp(path, hello_path) == 0) {
          stbuf->st_mode = S_IFREG | 0444;
          stbuf->st_nlink = 1;
          stbuf->st_size = strlen(hello_str);
      }
      else
          res = -ENOENT;
  
      return res;
} //always called before open
int ppfs_mknod(const char* path, mode_t mt, dev_t dt){}
int ppfs_mkdir(const char* path, mode_t mt){}
int ppfs_link(const char* path, const char* path2 ){}
int ppfs_opendir(const char* path, struct fuse_file_info* fi){
    syslog(LOG_WARNING, "ppfs_opendir path : %s", path);
}
int ppfs_open(const char* path, struct fuse_file_info* fi){
    syslog(LOG_WARNING, "ppfs_open path : %s", path);
    if(strcmp(path, hello_path) != 0){
        syslog(LOG_WARNING, "ppfs_open ENOENT");
          return -ENOENT;
    }
  
      if((fi->flags & 3) != O_RDONLY) {
        syslog(LOG_WARNING, "ppfs_open EACCES");
          return -EACCES;
      }
  
      return 0;
}
int ppfs_access (const char *path, int i){
    syslog(LOG_WARNING, "ppfs_access path : %s", path);
    ppacket *s = createpacket_s(4+strlen(path), CLTOMD_ACCESS,1);
    //syslog(LOG_WARNING, "createpacket_s packet size:%u, cmd:%u", s->size, s->cmd); //5,4096
    uint8_t* ptr = s->startptr+8;
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr+=strlen(path);
    sendpacket(fd, s);
    //syslog(LOG_WARNING, "sendpacket");
    free(s);
    s = receivepacket(fd);
    syslog(LOG_WARNING, "receivepacket");
    const uint8_t* ptr2 = s->startptr;
    int status = get32bit(&ptr2);
    syslog(LOG_WARNING,"access status:%d\n",status);
    if(status == 0){
        syslog(LOG_WARNING,"access success");
        print_attr((const attr*)ptr2);
    } else {
        if(status == -ENOENT){
            syslog(LOG_WARNING,"\tENOENT\n");
        }
    }
    free(s);
}
int	ppfs_chmod (const char *path, mode_t mt){
    syslog(LOG_WARNING, "ppfs_chmod path : %s", path);
    ppacket *s = createpacket_s(4+strlen(path)+4, CLTOMD_CHMOD,1);
    uint8_t* ptr = s->startptr+8;
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr+=strlen(path);
    put32bit(&ptr, mt);
    sendpacket(fd, s);
    free(s);
    s = receivepacket(fd);
    syslog(LOG_WARNING, "receivepacket");
    const uint8_t* ptr2 = s->startptr;
    int status = get32bit(&ptr2);
    syslog(LOG_WARNING,"chmod status:%d\n",status);
    if(status == 0){
        syslog(LOG_WARNING,"chmod success");
        print_attr((const attr*)ptr2);
    } else {
        if(status == -ENOENT){
            syslog(LOG_WARNING,"\tENOENT\n");
        }
        if(status == -ENOTDIR){
            syslog(LOG_WARNING,"\tENOTDIR\n");
        }
    }
    free(s);
}
int	ppfs_chown (const char *path, uid_t uid, gid_t gid){
    syslog(LOG_WARNING, "ppfs_chown path : %s", path);
    ppacket *s = createpacket_s(4+strlen(path)+8, CLTOMD_CHOWN, 1);
    uint8_t* ptr = s->startptr+8;
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr+=strlen(path);
    put32bit(&ptr, uid);
    put32bit(&ptr, gid);
    sendpacket(fd, s);
    free(s);
    s = receivepacket(fd);
    syslog(LOG_WARNING, "receivepacket");
    const uint8_t* ptr2 = s->startptr;
    int status = get32bit(&ptr2);
    syslog(LOG_WARNING,"chown status:%d\n",status);
    if(status == 0){
        syslog(LOG_WARNING,"chown success");
        print_attr((const attr*)ptr2);
    } else {
        if(status == -ENOENT){
            syslog(LOG_WARNING,"\tENOENT\n");
        }
    }
    free(s);

}
int	ppfs_create (const char *path, mode_t mt, struct fuse_file_info *fi){
    syslog(LOG_WARNING, "ppfs_create path : %s", path);
    
}
int	ppfs_flush (const char *path, struct fuse_file_info *fi){}
int	ppfs_fsync (const char *path, int i, struct fuse_file_info *fi){}
int	ppfs_read (const char * path, char * buf, size_t size, off_t offset, struct fuse_file_info *fi){
    size_t len;
      (void) fi;
      if(strcmp(path, hello_path) != 0)
          return -ENOENT;
      len = strlen(hello_str);
      if (offset < len) {
          if (offset + size > len)
              size = len - offset;
          memcpy(buf, hello_str + offset, size);
      } else
          size = 0;
      return size;
}
int	ppfs_readdir (const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi){
    syslog(LOG_WARNING, "ppfs_readdir path : %s", path);
    ppacket *s = createpacket_s(4+strlen(path)+4, CLTOMD_READDIR,1);
    uint8_t* ptr = s->startptr+8;
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr+=strlen(path);
    put32bit(&ptr, offset);
    sendpacket(fd, s);
    free(s);
    s = receivepacket(fd);
    syslog(LOG_WARNING, "receivepacket");
    const uint8_t* ptr2 = s->startptr;
    int status = get32bit(&ptr2);
    syslog(LOG_WARNING,"chmod status:%d\n",status);
    if(status == 0){
        syslog(LOG_WARNING,"chmod success");
        print_attr((const attr*)ptr2);
    } else {
        if(status == -ENOENT){
            syslog(LOG_WARNING,"\tENOENT\n");
        }
        if(status == -ENOTDIR){
            syslog(LOG_WARNING,"\tENOTDIR\n");
        }
    }
    free(s);
    (void) offset;
      (void) fi;
  
      if(strcmp(path, "/") != 0)
          return -ENOENT;
  
      filler(buf, ".", NULL, 0);
      filler(buf, "..", NULL, 0);
      filler(buf, hello_path + 1, NULL, 0);
  
      return 0;
}
int	ppfs_readlink (const char *path, char *buf, size_t st){}
int	ppfs_release (const char *path, struct fuse_file_info *fi){}
int	ppfs_releasedir (const char *path, struct fuse_file_info *fi){}
int	ppfs_rename (const char *path, const char *newpath){
    syslog(LOG_WARNING, "ppfs_rename path : %s", path);
    ppacket *s = createpacket_s(4+strlen(path)+4+strlen(newpath), CLTOMD_ACCESS,1);
    uint8_t* ptr = s->startptr+8;
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr+=strlen(path);
    put32bit(&ptr, strlen(newpath));
    memcpy(ptr, newpath, strlen(newpath));
    ptr+=strlen(newpath);
    sendpacket(fd, s);
    free(s);
    s = receivepacket(fd);
    syslog(LOG_WARNING, "receivepacket");
    const uint8_t* ptr2 = s->startptr;
    int status = get32bit(&ptr2);
    syslog(LOG_WARNING,"rename status:%d\n",status);
    if(status == 0){
        syslog(LOG_WARNING,"chmod success");
        print_attr((const attr*)ptr2);
    } else {
        if(status == -ENOENT){
            syslog(LOG_WARNING,"\tENOENT\n");
        }
    }
    free(s);

}
int	ppfs_rmdir (const char *path){}
int ppfs_statfs (const char *path, struct statvfs * st){}
int	ppfs_symlink (const char *path, const char *path2){}
int	ppfs_unlink (const char *path){}
int	ppfs_write (const char *path, const char *buf, size_t st, off_t off, struct fuse_file_info *fi){}

int main(int argc, char* argv[]) {
    return fuse_main(argc, argv, &ppfs_oper, NULL);
}
