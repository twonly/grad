#include "mds.h"
#include "datapack.h"
#include "mds_fs.h"

enum {KILL,HEADER,DATA};

static mdsserventry* mdsservhead = NULL;

static int lsock;
static int lsockpdescpos;

static mdsserventry* mdtomi = NULL;

static char* mishostip = "127.0.0.1";

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

	if (tcpstrlisten(lsock,"*",MDS_PORT_STR,100)<0) { //listen to client
		mfs_errlog(LOG_ERR,"mds: can't listen on socket");
		return -1;
	}
  
  fprintf(stderr,"listening on port %s\n",MDS_PORT_STR);

  inet_pton(AF_INET,mishostip,&misip);
  misip = htonl(misip);
  misport = MIS_PORT;

  msock = tcpsocket();
  if (tcpnodelay(msock)<0) {
    fprintf(stderr,"can't set TCP_NODELAY\n");
  }

  fprintf(stderr,"connecting to %u.%u.%u.%u:%d\n",(misip>>24)&0xFF,(misip>>16)&0xFF,(misip>>8)&0xFF,misip&0xFF,misport);
  if (tcpnumconnect(msock,misip,misport)<0) { //connect to mis
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

  mdtomi = eptr; //should not be the same link

	main_destructregister(mds_term);
  main_destructregister(term_fs);
	main_pollregister(mds_desc,mds_serve);

  return 0;
}

void mds_serve(struct pollfd *pdesc) {
	mdsserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			fprintf(stderr,"mds: accept error\n");
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

      fflush(stderr);
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

	fprintf(stderr,"mds: closing %s:%s\n","*",MDS_PORT_STR);
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
      if(id == -1) //little hack in order for mis to pretend to be a client forwarding chunk related packets
        id = eptr->peerip;

      ppacket* inp = createpacket_r(size,cmd,id);
      inp->next = eptr->inpacket;
      eptr->inpacket = inp;

      eptr->mode = DATA;

      fprintf(stderr,"got packet header,size=%d,cmd=%X,id=(%u.%u.%u.%u),bytesleft=%d\n",size,cmd,(id>>24)&0xFF,(id>>16)&0xFF,(id>>8)&0xFF,id&0xFF,inp->bytesleft);
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

    case CLTOMD_CHMOD:
      mds_chmod(eptr,p);
      break;
    case MITOMD_CHMOD:
      mds_cl_chmod(eptr,p);
      break;
    case CLTOMD_CHOWN:
      mds_chown(eptr,p);
      break;
    case MITOMD_CHOWN:
      mds_cl_chown(eptr,p);
      break;
    case CLTOMD_CHGRP:
      mds_chgrp(eptr,p);
      break;
    case MITOMD_CHGRP:
      mds_cl_chgrp(eptr,p);
      break;

    case CLTOMD_CREATE:
      mds_create(eptr,p);
      break;
    case MITOMD_CREATE:
      mds_cl_create(eptr,p);
      break;
    case CLTOMD_OPEN:
      mds_open(eptr,p);
      break;
    case MITOMD_OPEN:
      mds_cl_open(eptr,p);
      break;

    case CLTOMD_READ_CHUNK_INFO:
      mds_cl_read_chunk_info(eptr,p);
      break;
    case CLTOMD_LOOKUP_CHUNK:
      mds_cl_lookup_chunk(eptr,p);
      break;
    case CLTOMD_APPEND_CHUNK:
      mds_cl_append_chunk(eptr,p);
      break;

    case MITOMD_READ_CHUNK_INFO:
      mds_fw_read_chunk_info(eptr,p);
      break;

    case CLTOMD_POP_CHUNK:
      mds_cl_pop_chunk(eptr,p);
      break;
  }

  fprintf(stderr,"\n\n");
}

mdsserventry* mds_entry_from_id(int id){ //maybe add a hash?
  mdsserventry* eptr = mdsservhead;
  while(eptr){
    if(eptr->peerip == id)
      return eptr;

    eptr = eptr->next;
  }

  return eptr;
}

static void mds_direct_pass_cl(mdsserventry* eptr,ppacket* p,int cmd){
  mdsserventry* ceptr = mds_entry_from_id(p->id);

  if(ceptr){
    ppacket* outp = createpacket_s(p->size,cmd,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = ceptr->outpacket;
    ceptr->outpacket = outp;
  }
}

static void mds_direct_pass_mi(ppacket* p,int cmd){
  const uint8_t* ptr = p->startptr;

  ppacket* outp = createpacket_s(p->size,cmd,p->id);
  memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

  outp->next = mdtomi->outpacket;
  mdtomi->outpacket = outp;
}

static void mds_update_attr(ppacket* p,ppfile* f){
  int plen = strlen(f->path);
  ppacket* outp = createpacket_s(4+plen+sizeof(attr),MDTOMI_UPDATE_ATTR,p->id);
  uint8_t* ptr2 = outp->startptr + HEADER_LEN;

  put32bit(&ptr2,plen);

  memcpy(ptr2,f->path,plen);
  ptr2 += plen;

  memcpy(ptr2,&f->a,sizeof(attr));

  outp->next = mdtomi->outpacket;
  mdtomi->outpacket = outp;
}

void mds_getattr(mdsserventry* eptr,ppacket* p){
  int plen;
  const uint8_t* ptr = p->startptr;
  plen = get32bit(&ptr);
  syslog(LOG_WARNING, "mds_getattr: %u", plen);
  char* path = (char*)malloc(plen+1);
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

  free(path);
}

void mds_cl_getattr(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_GETATTR);
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

  free(path);
}

void mds_cl_readdir(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_READDIR);
}

void mds_chmod(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_chmod\n");

  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  printf("plen=%d\n",plen);

  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;
  path[plen] = 0;

  fprintf(stderr,"path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    ppacket* outp = createpacket_s(p->size,MDTOMI_CHMOD,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = mdtomi->outpacket;
    mdtomi->outpacket = outp;
  } else {
    int perm = get32bit(&ptr);
    f->a.mode = (perm&0777) | (f->a.mode & (~0777));

    fprintf(stderr,"perm=%d%d%d\n",perm/0100 & 7,
                                   perm/0010 & 7,
                                   perm & 7);

    ppacket* outp = createpacket_s(4,MDTOCL_CHMOD,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;
    
    put32bit(&ptr2,0);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;

    mds_update_attr(p,f);
  }

  free(path);
}

void mds_cl_chmod(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_CHMOD);
}

void mds_chown(mdsserventry* eptr,ppacket* p){
  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;
  path[plen] = 0;

  fprintf(stderr,"path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    ppacket* outp = createpacket_s(p->size,MDTOMI_CHMOD,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = mdtomi->outpacket;
    mdtomi->outpacket = outp;
  } else {
    int uid = get32bit(&ptr);
    f->a.uid = uid;

    fprintf(stderr,"uid = %u\n",uid);

    ppacket* outp = createpacket_s(4,MDTOCL_CHMOD,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;
    
    put32bit(&ptr2,0);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;

    mds_update_attr(p,f);
  }

  free(path);
}

void mds_cl_chown(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_CHOWN);
}

void mds_chgrp(mdsserventry* eptr,ppacket* p){
  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;
  path[plen] = 0;

  fprintf(stderr,"path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    ppacket* outp = createpacket_s(p->size,MDTOMI_CHMOD,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = mdtomi->outpacket;
    mdtomi->outpacket = outp;
  } else {
    int gid = get32bit(&ptr);
    f->a.gid = gid;

    fprintf(stderr,"gid=%u\n",gid);

    ppacket* outp = createpacket_s(4,MDTOCL_CHMOD,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;
    
    put32bit(&ptr2,0);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;

    mds_update_attr(p,f);
  }

  free(path);
}

void mds_cl_chgrp(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_cl(eptr,inp,MDTOCL_CHGRP);
}

void mds_create(mdsserventry* eptr,ppacket* p){
    int plen;
    const uint8_t* ptr = p->startptr;

    plen = get32bit(&ptr);
    char* path = (char*)malloc(plen+10);
    memcpy(path,ptr,plen);
    path[plen] = 0;
    ptr += plen;
    mode_t mt = get32bit(&ptr);

    fprintf(stderr,"path:%s\n",path);
    ppfile* f = lookup_file(path);
    if(f == NULL){ //not exist, ask MIS
//        attr a;
//        a.uid = a.gid = 0;
//        a.atime = a.ctime = a.mtime = time(NULL);
//        a.link = 1;
//        a.size = 0;
//        //a.mode = 0777 | S_IFREG;
//        a.mode = mt | S_IFREG; //no umask
//        add_file(new_file(path,a));
//        fprintf(stderr,"add file path:%s\n",path);

        //ppacket* outp = createpacket_s(4, MDTOCL_CREATE,p->id);
        //uint8_t *ptr2 = outp->startptr + HEADER_LEN;
        //put32bit(&ptr2,0); //status
//
//        outp->next = eptr->outpacket;
//        eptr->outpacket = outp;
        fprintf(stderr,"inquery MIS path:%s\n",path);

        ppacket* outpi = createpacket_s(p->size,MDTOMI_CREATE,p->id); //inquery MIS
        memcpy(outpi->startptr+HEADER_LEN,p->startptr,p->size);

        outpi->next = mdtomi->outpacket;
        mdtomi->outpacket = outpi;
    } else { //already exist, return EEXIST
        ppacket* outp2 = createpacket_s(4, MDTOCL_CREATE, p->id);
        uint8_t *ptr3 = outp2->startptr + HEADER_LEN;
        put32bit(&ptr3, -EEXIST);
        fprintf(stderr,"file path:%s exist; status :%d\n",path, -EEXIST);
        outp2->next = eptr->outpacket;
        eptr->outpacket = outp2;
    }
    free(path);
    //mds_direct_pass_mi(p,MDTOMI_CREATE);

}

void mds_cl_create(mdsserventry* eptr,ppacket* inp){
    //handle the message
    const uint8_t *ptr = inp->startptr;
    int status = get32bit(&ptr);
    
    if(status==0) { //create locally
        int plen = get32bit(&ptr);
        char *path = (char*)malloc((plen+1)*sizeof(char));
        memcpy(path, ptr, plen);
        path[plen] = 0;
        ptr += plen;
        fprintf(stderr,"mds create path:%s locally\n",path );
        attr a;
        a.uid = a.gid = 0;
        a.atime = a.ctime = a.mtime = time(NULL);
        a.link = 1;
        a.size = 0;
        a.mode = 0777 | S_IFREG;
        ppfile* nf = new_file(path,a);
        add_file(nf);
        //nf->next = f->child; //should mds maintain the edges?
        //f->child = nf;
        nf->srcip = eptr->peerip;

        //p = createpacket_s(4,MITOMD_CREATE,inp->id);
        //uint8_t* ptr = p->startptr + HEADER_LEN;
        //put32bit(&ptr,0);
    }
  mds_direct_pass_cl(eptr,inp,MDTOCL_CREATE);
}

void mds_open(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_mi(inp,MDTOMI_OPEN);
}

void mds_cl_open(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_cl(eptr,inp,MDTOCL_OPEN);
}

void mds_cl_read_chunk_info(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_read_chunk_info\n");

  int plen,mdsid,i;
  const uint8_t* ptr = p->startptr;
  ppacket* outp = NULL;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;
  
  if(mdtomi == eptr)
    mdsid = get32bit(&ptr);

  path[plen] = 0;
  fprintf(stderr,"path=%s\n",path);
  ppfile* f = lookup_file(path);
  if(f == NULL){
    if(eptr != mdtomi){//file in another mds!
      mds_direct_pass_mi(p,MDTOMI_READ_CHUNK_INFO);
    } else { //no such file
      outp = createpacket_s(4+4,MDTOCL_READ_CHUNK_INFO,p->id);
      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,-ENOENT);
      put32bit(&ptr2,mdsid);
    }
  } else {
    int totsize = 4+4+4+8*(f->chunks);
    if(eptr == mdtomi){
      totsize += 4;
    }

    outp = createpacket_s(totsize,MDTOCL_READ_CHUNK_INFO,p->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;
    put32bit(&ptr,0);

    put32bit(&ptr,-1);//local mds
    put32bit(&ptr,f->chunks);
    for(i=0;i<f->chunks;i++){
      put64bit(&ptr,f->clist[i]);
    }

    fprintf(stderr,"chunks=%d\n",f->chunks);

    if(eptr == mdtomi){
      put32bit(&ptr,mdsid);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}

void mds_cl_lookup_chunk(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_lookup_chunk\n");

  const uint8_t* ptr = p->startptr;
  uint64_t chunkid = get64bit(&ptr);
  int mdsid;
  ppacket* outp = NULL;

  mdschunk* c = lookup_chunk(chunkid);

  if(c == NULL){
    outp = createpacket_s(4+4,MDTOCL_LOOKUP_CHUNK,p->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;
    put32bit(&ptr2,-ENOENT);
  } else {
    int totsize = 4+4;

    outp = createpacket_s(totsize,MDTOCL_LOOKUP_CHUNK,p->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;
    put32bit(&ptr,0);
    put32bit(&ptr,c->csip);
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}

void mds_cl_append_chunk(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_append_chunk\n");

  int plen,i;
  const uint8_t* ptr = p->startptr;
  ppacket* outp = NULL;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;

  path[plen] = 0;
  fprintf(stderr,"path=%s\n",path);
  ppfile* f = lookup_file(path);

  if(f == NULL){
    outp = createpacket_s(4,MDTOCL_APPEND_CHUNK,p->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;
    put32bit(&ptr2,-ENOENT);
  } else {
    mdschunk* c;
    mdscs_new_chunk(&c);
    int ret = 0;
    uint64_t chunkid;

    if(c){
      add_chunk(c);

      ret = mdscs_append_chunk(f,c);
      chunkid = c->chunkid;

      fprintf(stderr,"mdscs_append_chunk,ret=%d,chunkid=%lld\n",ret,chunkid);
    }

    if(ret != 0){
      outp = createpacket_s(4,MDTOCL_APPEND_CHUNK,p->id);

      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,ret);
    } else if(c == NULL){
      outp = createpacket_s(4,MDTOCL_APPEND_CHUNK,p->id);

      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,-ENOSPC);
    } else {
      int totsize = 4+8;

      outp = createpacket_s(totsize,MDTOCL_APPEND_CHUNK,p->id);
      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,0);
      put64bit(&ptr2,chunkid);

      mds_update_attr(p,f);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}

void mds_fw_read_chunk_info(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_fw_read_chunk_info\n");
  mds_direct_pass_cl(eptr,p,MDTOCL_READ_CHUNK_INFO);
}

void mds_cl_pop_chunk(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_pop_chunk\n");

  const uint8_t* ptr = p->startptr;
  ppacket* outp = NULL;

  int plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;

  path[plen] = 0;
  fprintf(stderr,"path=%s\n",path);
  ppfile* f = lookup_file(path);

  if(f == NULL){
    outp = createpacket_s(4+4,MDTOCL_POP_CHUNK,p->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;
    put32bit(&ptr2,-ENOENT);
  } else {
    int ret;
    uint64_t chunkid;

    ret = mdscs_pop_chunk(f,&chunkid);

    fprintf(stderr,"mdscs_pop_chunk,ret=%d,chunkid=%lld\n",ret,chunkid);

    if(ret != 0){
      outp = createpacket_s(4,MDTOCL_POP_CHUNK,p->id);

      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,ret);
    } else {
      mdscs_delete_chunk(chunkid);

      int totsize = 4+8;

      outp = createpacket_s(totsize,MDTOCL_POP_CHUNK,p->id);
      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,0);
      put64bit(&ptr2,chunkid);

      //send update_attr to mis
      mds_update_attr(p,f);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}
