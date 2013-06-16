#include "mds.h"
#include "datapack.h"
#include "mds_fs.h"

mdsserventry* mdsservhead = NULL;

int lsock;
int lsockpdescpos;

mdsserventry* mdtomi = NULL;

char* mishostip = "127.0.0.1";

#define MDS_FILES 2
char* mds_test[] = {"/a","/b"};

void mds_fs_demo_init(void){
  attr a;
  int i;
  ppfile* f;

  a.uid = a.gid = 0;
  a.atime = a.ctime = a.mtime = time(NULL);
  a.link = 1;
  a.size = 12345;
  a.mode = 0777 | S_IFDIR;

  for(i=0;i<MDS_FILES;i++){
    ppfile* f = new_file(mds_test[i],a);
    add_file(f);
  }
}

int mds_init(void){
  int misip,misport;
  int msock;

  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"mds: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);

  lsockpdescpos = -1;

	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"mds: can't set accept filter");
	}

	if (tcpstrlisten(lsock,"*","8124",100)<0) {
		mfs_errlog(LOG_ERR,"mds: can't listen on socket");
		return -1;
	}
  
  fprintf(stderr,"listening on port 8124\n");

  inet_pton(AF_INET,mishostip,&misip);
  misip = htonl(misip);
  misport = MIS_PORT;

  msock = tcpsocket();
  if (tcpnodelay(msock)<0) {
    fprintf(stderr,"can't set TCP_NODELAY\n");
  }

  fprintf(stderr,"connecting to %u.%u.%u.%u:%d\n",(misip>>24)&0xFF,(misip>>16)&0xFF,(misip>>8)&0xFF,misip&0xFF,misport);
  if (tcpnumconnect(msock,misip,misport)<0) {
    fprintf(stderr,"can't connect to mis (\"%X\":\"%"PRIu16"\")\n",misip,misport);
    tcpclose(msock);
    return -1;
  }

	mdsserventry* eptr = malloc(sizeof(mdsserventry));
	passert(eptr);

	eptr->sock = msock;
	eptr->pdescpos = -1;

  eptr->peerip = misip;
  eptr->mode = HEADER;

  eptr->inpacket = NULL;
  eptr->outpacket = NULL;
  eptr->bytesleft = HEADER_LEN;
  eptr->startptr = eptr->headbuf;

  mdtomi = eptr;

	main_destructregister(mds_term);
  main_destructregister(term_fs);
	main_pollregister(mds_desc,mds_serve);

  mds_fs_demo_init();

  return 0;
}

void mds_serve(struct pollfd *pdesc) {
	mdsserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"mds: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(mdsserventry));
			passert(eptr);

			eptr->next = mdsservhead;
			mdsservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      fprintf(stderr,"client(ip:%u.%u.%u.%u) connected\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
		}
	}

  eptr = mdtomi;
  if (eptr->pdescpos>=0) {
    if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
      eptr->mode = KILL;
    }
    if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
      mds_read(eptr);
    }
  }

  eptr = mdtomi;
  if (eptr->pdescpos>=0) {
    if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
      mds_write(eptr);
    }
  }

// read
	for (eptr=mdsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				mds_read(eptr);
			}
		}
	}

// write
	for (eptr=mdsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				mds_write(eptr);
			}
		}
	}

	mdsserventry** kptr = &mdsservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			tcpclose(eptr->sock);

      ppacket *pp,*ppn;
      for( pp = eptr->inpacket; pp; pp = ppn){
        ppn = pp->next;
        free(pp);
      }
      for( pp = eptr->outpacket; pp; pp = ppn){
        ppn = pp->next;
        free(pp);
      }

			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}

  if(mdtomi->mode == KILL){//Oops
    fprintf(stderr,"connection to mis closed\n");
    //maybe reconnect?
    kill(getpid(),SIGINT); //interrupt
  }
}

void mds_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	mdsserventry *eptr;

  pdesc[pos].fd = lsock;
  pdesc[pos].events = POLLIN;
  lsockpdescpos = pos;
  pos++;

	for(eptr=mdsservhead ; eptr ; eptr=eptr->next){
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;

		pdesc[pos].events |= POLLIN;
		if (eptr->outpacket != NULL) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}

  eptr = mdtomi;
  pdesc[pos].fd = eptr->sock;
  pdesc[pos].events = 0;
  eptr->pdescpos = pos;

  pdesc[pos].events |= POLLIN;
  if (eptr->outpacket != NULL) {
    pdesc[pos].events |= POLLOUT;
  }
  pos++;

	*ndesc = pos;
}

void mds_term(void) {
	mdsserventry *eptr,*eptrn;

	fprintf(stderr,"mds: closing %s:%s\n","*","8124");
	tcpclose(lsock);
  tcpclose(mdtomi->sock);

	for (eptr = mdsservhead ; eptr ; eptr = eptrn) {
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

  eptr = mdtomi;
  {
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

void mds_read(mdsserventry *eptr) {
	int i;
  int size,cmd,id;

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
      id = get32bit(&pptr);

      ppacket* inp = createpacket_r(size,cmd,id);
      inp->next = eptr->inpacket;
      eptr->inpacket = inp;

      eptr->mode = DATA;

      fprintf(stderr,"got packet header,size=%d,cmd=%X,id=%d,bytesleft=%d\n",size,cmd,id,inp->bytesleft);
      continue;
    } else {
      eptr->inpacket->bytesleft -= i;
      eptr->inpacket->startptr += i;

      if(eptr->inpacket->bytesleft > 0) return;

      eptr->inpacket->startptr = eptr->inpacket->buf;

      fprintf(stderr,"packet received,size=%d,cmd=%X,id=%d\n",eptr->inpacket->size,eptr->inpacket->cmd,eptr->inpacket->id);

      mds_gotpacket(eptr,eptr->inpacket);
      ppacket* p = eptr->inpacket;
      eptr->inpacket = eptr->inpacket->next;
      free(p);

      if(eptr->inpacket == NULL){
        eptr->mode = HEADER;
        eptr->startptr = eptr->headbuf;
        eptr->bytesleft = HEADER_LEN;
      }

      return;
    }

	}
}

void mds_write(mdsserventry *eptr) {
	int32_t i;

  while(eptr->outpacket){
		i=write(eptr->sock,eptr->outpacket->startptr,eptr->outpacket->bytesleft);

		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"mds: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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

void mds_gotpacket(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"dispatching packet ,size:%d,cmd:%X\n",p->size,p->cmd);
  switch(p->cmd){
    case MITOMD_GETATTR:
      mds_cl_getattr(eptr,p);
      break;

    case MITOMD_READDIR:
      mds_cl_readdir(eptr,p);
      break;

    case CLTOMD_GETATTR:
      mds_getattr(eptr,p);
      break;
    case CLTOMD_READDIR:
      mds_readdir(eptr,p);
      break;
  }
}

mdsserventry* mds_entry_from_id(int id){ //maybe add a hash?
  mdsserventry* eptr = mdsservhead;
  while(eptr){
    if(eptr->peerip == htonl(id))
      return eptr;

    eptr = eptr->next;
  }

  return eptr;
}


void mds_getattr(mdsserventry* eptr,ppacket* p){
  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  path[plen] = 0;

  fprintf(stderr,"path:%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    ppacket* outp = createpacket_s(p->size,MDTOMI_GETATTR,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = mdtomi->outpacket;
    mdtomi->outpacket = outp;
  } else {
    ppacket* outp = createpacket_s(4+sizeof(attr),MDTOCL_GETATTR,p->id);
    uint8_t *ptr2 = outp->startptr + HEADER_LEN;

    put32bit(&ptr2,0); //status
    memcpy(ptr2,&f->a,sizeof(attr));

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}

void mds_cl_getattr(mdsserventry* eptr,ppacket* p){
  mdsserventry* ceptr = mds_entry_from_id(p->id);

  if(ceptr){
    ppacket* outp = createpacket_s(p->size,MDTOCL_GETATTR,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = ceptr->outpacket;
    ceptr->outpacket = outp;
  }
}

void mds_readdir(mdsserventry* eptr,ppacket* p){
  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  path[plen] = 0;

  fprintf(stderr,"path:%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    ppacket* outp = createpacket_s(p->size,MDTOMI_READDIR,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = mdtomi->outpacket;
    mdtomi->outpacket = outp;
  } else {
    ppacket* outp = createpacket_s(4,MDTOCL_READDIR,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;
    
    put32bit(&ptr2,-ENOTDIR);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }

}

void mds_cl_readdir(mdsserventry* eptr,ppacket* p){
  mdsserventry* ceptr = mds_entry_from_id(p->id);

  const uint8_t* ptr = p->startptr;
  int status = get32bit(&ptr);
  printf("status=%d\n",status);

  if(status == 0){
    int nfiles = get32bit(&ptr);
    printf("%d files\n",nfiles);
  }

  if(ceptr){
    ppacket* outp = createpacket_s(p->size,MDTOCL_READDIR,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = ceptr->outpacket;
    ceptr->outpacket = outp;
  }
}
