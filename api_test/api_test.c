#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "ppcomm.h"
#include "ppfile.h"
#include "datapack.h"

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
  printf("size=%d,cmd=%X,id=%d\n",p->size,p->cmd,p->id);
}

ppacket* receivepacket(int sockfd){
  char headbuf[20];
  uint8_t* startptr;
  const uint8_t* ptr;
  int i,hleft;
  ppacket* p;

  hleft = HEADER_LEN;
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

  int size,cmd,id;
  ptr = headbuf;
  size = get32bit(&ptr);
  cmd = get32bit(&ptr);
  id = get32bit(&ptr);

  printf("got packet:size=%d,cmd=%X,id=%d\n",size,cmd,id);
  p = createpacket_r(size,cmd,id);
  
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
    printf("\tdirectory\n");
  } else if(a->mode & S_IFREG){
    printf("\tregular file\n");
  }


  printf("\tperm:%d%d%d\n",a->mode / 0100 & 7,
                           a->mode / 0010 & 7,
                           a->mode & 7);

  printf("\tuid=%d,gid=%d",a->uid,a->gid);
  printf("\tatime=%d,ctime=%d,mtime=%d\n",a->atime,a->ctime,a->mtime);
  printf("\tlink=%d\n",a->link);
  printf("\tsize=%d\n",a->size);
}

int main(void){
  int sockfd = socket(AF_INET,SOCK_STREAM,0);
  int i,ip;
  struct sockaddr_in servaddr;
  char path[100],cmd[100],buf[200];

  memset(&servaddr,0,sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  inet_pton(AF_INET,"127.0.0.1",&servaddr.sin_addr);
  inet_pton(AF_INET,"127.0.0.1",&ip);
  servaddr.sin_port = htons(MDS_PORT);
  if(connect(sockfd,(struct sockaddr*)&servaddr,sizeof(servaddr)) != 0){
    perror("cannot connect to mds");
    exit(1);
  }

  printf(">>>");
  while(scanf("%s%s",cmd,path)!=EOF){
    if(!strcmp(cmd,"getattr")){
      ppacket* p = createpacket_s(4+strlen(path),CLTOMD_GETATTR,ip);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,strlen(path));
      memcpy(ptr,path,strlen(path));
      sendpacket(sockfd,p);
      free(p);

      p = receivepacket(sockfd);

      const uint8_t* ptr2 = p->startptr;
      int status = get32bit(&ptr2);
      printf("status:%d\n",status);
      if(status == 0){
        print_attr((const attr*)ptr2);
      } else {
        if(status == -ENOENT){
          printf("\tENOENT\n");
        }
        if(status == -ENOTDIR){
          printf("\tENOTDIR\n");
        }
      }
    }
    if(!strcmp(cmd,"readdir")){
      ppacket* p = createpacket_s(4+strlen(path),CLTOMD_READDIR,ip);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,strlen(path));
      memcpy(ptr,path,strlen(path));
      sendpacket(sockfd,p);
      free(p);

      p = receivepacket(sockfd);

      const uint8_t* ptr2 = p->startptr;
      int status = get32bit(&ptr2);
      printf("status:%d\n",status);
      if(status == 0){
        int n = get32bit(&ptr2);
        printf("%d files:\n",n);
        for(i=0;i<n;i++){
          int len = get32bit(&ptr2);
          memcpy(buf,ptr2,len);
          ptr2 += len;

          buf[len] = 0;
          printf("\t%s\n",buf);
        }
      } else {
        if(status == -ENOENT){
          printf("\tENOENT\n");
        }
        if(status == -ENOTDIR){
          printf("\tENOTDIR\n");
        }
      }
    }

    printf(">>>");
  }

  close(sockfd);
  return 0;
}
