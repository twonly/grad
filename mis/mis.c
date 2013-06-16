#include "mis.h"
#include "datapack.h"

misserventry* misservhead = NULL;

int lsock;
int lsockpdescpos;

int mis_init(void){
  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"main master server module: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);

  lsockpdescpos = -1;

	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,"*","8123",100)<0) {
		mfs_errlog(LOG_ERR,"main master server module: can't listen on socket");
		return -1;
	}
  
  fprintf(stderr,"listening on port 8080\n");

	main_destructregister(mis_term);
	main_pollregister(mis_desc,mis_serve);

  return 0;
}

void mis_serve(struct pollfd *pdesc) {
	misserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"main master server module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(misserventry));
			passert(eptr);

			eptr->next = misservhead;
			misservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = 8;
      eptr->startptr = eptr->headbuf;
		}
	}

// read
	for (eptr=misservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				mis_read(eptr);
			}
		}
	}

// write
	for (eptr=misservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				mis_write(eptr);
			}
		}
	}

	misserventry** kptr = &misservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			tcpclose(eptr->sock);
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void mis_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	misserventry *eptr;

  pdesc[pos].fd = lsock;
  pdesc[pos].events = POLLIN;
  lsockpdescpos = pos;
  pos++;

	for(eptr=misservhead ; eptr ; eptr=eptr->next){
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;

		pdesc[pos].events |= POLLIN;
		if (eptr->outpacket != NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}
	*ndesc = pos;
}

void mis_term(void) {
	misserventry *eptr,*eptrn;

	fprintf(stderr,"main master server module: closing %s:%s\n","*","8080");
	tcpclose(lsock);

	for (eptr = misservhead ; eptr ; eptr = eptrn) {
    ppacket *pp,*ppn;

		eptrn = eptr->next;

    for( pp = eptr->inpacket; pp; pp = ppn){
      ppn = pp->next;
      free(pp);
    }

    for( pp = eptr->outpacket; pp; pp = ppn){
      ppn = pp->next;
      free(pp);
    }

		free(eptr);
	}
}

void mis_read(misserventry *eptr) {
	int i;
  int size,cmd;

	while(1){
    if(eptr->mode == HEADER){
		  i=read(eptr->sock,eptr->startptr,eptr->bytesleft);
    } else {
      i=read(eptr->sock,eptr->inpacket->startptr,eptr->inpacket->bytesleft);
    }

		if (i==0) {
      fprintf(stderr,"connection with client(ip:%u.%u.%u.%u) has been closed by peer\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
			eptr->mode = KILL;
			return;
		}

		if (i<0) {
			if (errno!=EAGAIN) {
				eptr->mode = KILL;
			}
			return;
		}

    //debug
    fprintf(stderr,"read %d from (ip:%u.%u.%u.%u)\n",i,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

    if(eptr->mode == HEADER){
      eptr->bytesleft -= i;
      eptr->startptr += i;
      if(eptr->bytesleft > 0) return;

      const uint8_t *pptr = eptr->headbuf;
      size = get32bit(&pptr);
      cmd = get32bit(&pptr);
      fprintf(stderr,"got packet header,size=%d,cmd=%X\n",size,cmd);

      ppacket* inp = createpacket_r(size,cmd);
      inp->next = eptr->inpacket;
      eptr->inpacket = inp;

      eptr->mode = DATA;

      continue;
    } else {
      eptr->inpacket->bytesleft -= i;
      eptr->inpacket->startptr += i;

      if(eptr->inpacket->bytesleft > 0) return;

      eptr->inpacket->startptr = eptr->inpacket->buf;

      fprintf(stderr,"packet received,size=%d,cmd=%X\n",eptr->inpacket->size,eptr->inpacket->cmd);

      mis_gotpacket(eptr,eptr->inpacket);
      ppacket* p = eptr->inpacket;
      eptr->inpacket = eptr->inpacket->next;
      free(p);

      if(eptr->inpacket == NULL){
        eptr->mode = HEADER;
        eptr->startptr = eptr->headbuf;
        eptr->bytesleft = 8;
      }

      return;
    }

	}
}

void mis_write(misserventry *eptr) {
	int32_t i;

  fprintf(stderr,"holy crap\n");

  while(eptr->outpacket){
		i=write(eptr->sock,eptr->outpacket->startptr,eptr->outpacket->bytesleft);

		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
				eptr->mode = KILL;
			}
			return;
		}

    //debug
    fprintf(stderr,"wrote %d from (ip:%u.%u.%u.%u)\n",i,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

		eptr->outpacket->startptr += i;
		eptr->outpacket->bytesleft -=i;

    if(eptr->outpacket->bytesleft > 0) return;

    ppacket* p = eptr->outpacket;
    eptr->outpacket = eptr->outpacket->next;
    free(p);
	}
}

void mis_gotpacket(misserventry* eptr,ppacket* p){
  fprintf(stderr,"dispatching packet ,size:%d,cmd:%d\n",p->size,p->cmd);
  switch(p->cmd){
    case MDTOMI_GETATTR:
      mis_getattr(eptr,p);
      break;
    case MDTOMI_READDIR:
      mis_readdir(eptr,p);
      break;
  }
}

#define ROOT_DIR_FILES 4
char* root_test[] = {"a","b","c","d"};

void mis_getattr(misserventry* eptr,ppacket* inp){
  char* path;
  int len;
  int i;
  ppacket* p;
  const uint8_t* inptr;

  inptr = inp->startptr;
  len = get32bit(&inptr);
  printf("plen=%X\n",len);

  path = (char*)malloc((len+10)*sizeof(char));
  memcpy(path,inptr,len*sizeof(char));
  path[len] = 0;

  printf("path=%s\n",path);

  attr s;
  s.uid = s.gid = 0;
  s.atime = s.ctime = s.mtime = time(NULL);
  s.link = 1;
  s.size = 12345;

  //just for test
  if(!strcmp(path,"/")){
    p = createpacket_s(4+sizeof(attr),MITOMD_GETATTR);
    uint8_t* ptr = p->startptr + 8;

    put32bit(&ptr,0);//status

    s.mode = 0777 | S_IFDIR;
    memcpy(ptr,&s,sizeof(attr)); //directory attr
  } else {
    for(i=0;i<ROOT_DIR_FILES;i++){
      if(!strcmp(path+1,root_test[i])){
        p = createpacket_s(4+sizeof(attr),MITOMD_GETATTR);
        uint8_t *ptr = p->startptr + 8;

        put32bit(&ptr,0); //status
        s.mode = 0777 | S_IFREG;
        memcpy(ptr,&s,sizeof(attr));
        break;
      }
    }

    if(i == 4){ //unknown file
      p = createpacket_s(4,MITOMD_GETATTR);
      uint8_t *ptr = p->startptr + 8;

      put32bit(&ptr,-ENOENT);
    }
  }


  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_readdir(misserventry* eptr,ppacket* inp){
  ppacket* p;
  char* path;
  int len;
  const uint8_t* inptr;
  int i;

  inptr = inp->startptr;
  len = get32bit(&inptr);
  printf("plen=%d\n",len);

  path = (char*)malloc((len+10)*sizeof(char));
  memcpy(path,inptr,len*sizeof(char));
  path[len] = 0;

  printf("path=%s\n",path);

  if(!strcmp(path,"/")){
    int totsize = 8;

    for(i=0;i<ROOT_DIR_FILES;i++){
      totsize += 4;
      totsize += strlen(root_test[i]);
    }

    fprintf(stderr,"readdir send total size:%d\n",totsize);

    p = createpacket_s(totsize,MITOMD_READDIR);
    uint8_t *ptr = p->startptr + 8;
    put32bit(&ptr,0); //status

    put32bit(&ptr,ROOT_DIR_FILES); //number of files

    for(i=0;i<ROOT_DIR_FILES;i++){
      put32bit(&ptr,strlen(root_test[i]));
      memcpy(ptr,root_test[i],strlen(root_test[i]));
      ptr += strlen(root_test[i]);
    }
  } else {
    for(i=0;i<ROOT_DIR_FILES;i++){
      if(!strcmp(path+1,root_test[i])){
        p = createpacket_s(4,MITOMD_READDIR);
        uint8_t *ptr = p->startptr+8;

        put32bit(&ptr,-ENOTDIR);
        break;
      }
    }

    if(i == 4){ //unknown file
      p = createpacket_s(4,MITOMD_GETATTR);
      uint8_t *ptr = p->startptr + 8;

      put32bit(&ptr,-ENOENT);
    }
  }

  p->next = eptr->outpacket;
  eptr->outpacket = p;
}
