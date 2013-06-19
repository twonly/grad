#include "mdscs.h"
#include "datapack.h"
#include "mds_fs.h"

enum {KILL,HEADER,DATA};

static mdscsserventry* mdscsservhead = NULL;

static int lsock;
static int lsockpdescpos;

static uint64_t global_chunk_counter;

int mdscs_init(void){
  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"mdscs module: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);

  lsockpdescpos = -1;

  if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
    mfs_errlog_silent(LOG_NOTICE,"mdscs module: can't set accept filter");
  }

	if (tcpstrlisten(lsock,"*",MDSCS_PORT_STR,100)<0) {
		mfs_errlog(LOG_ERR,"mdscs module: can't listen on socket");
		return -1;
	}

  fprintf(stderr,"mdscs listening on port %s\n",MDSCS_PORT_STR);

  global_chunk_counter = 1;

	main_destructregister(mdscs_term);
	main_pollregister(mdscs_desc,mdscs_serve);

  return 0;
}

void mdscs_serve(struct pollfd *pdesc) {
	mdscsserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"mdscs module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(mdscsserventry));
			passert(eptr);

			eptr->next = mdscsservhead;
			mdscsservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      eptr->clist = NULL;
      eptr->chunks = eptr->availspace = eptr->space = -1;

      fprintf(stderr,"cs(ip:%u.%u.%u.%u) connected\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
		}
	}

// read
	for (eptr=mdscsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				mdscs_read(eptr);
			}
		}
	}

// write
	for (eptr=mdscsservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				mdscs_write(eptr);
			}
		}
	}

	mdscsserventry** kptr = &mdscsservhead;
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

void mdscs_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	mdscsserventry *eptr;

  pdesc[pos].fd = lsock;
  pdesc[pos].events = POLLIN;
  lsockpdescpos = pos;
  pos++;

	for(eptr=mdscsservhead ; eptr ; eptr=eptr->next){
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

void mdscs_term(void) {
	mdscsserventry *eptr,*eptrn;

	fprintf(stderr,"mdscs module: closing %s:%s\n","*",MDSCS_PORT_STR);
	tcpclose(lsock);

	for (eptr = mdscsservhead ; eptr ; eptr = eptrn) {
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

void mdscs_read(mdscsserventry *eptr) {
	int i;
  int size,cmd,id;

	while(1){
    if(eptr->mode == HEADER){
		  i=read(eptr->sock,eptr->startptr,eptr->bytesleft);
    } else {
      i=read(eptr->sock,eptr->inpacket->startptr,eptr->inpacket->bytesleft);
    }

		if (i==0) {
      fprintf(stderr,"connection with cs(ip:%u.%u.%u.%u) has been closed by peer\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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
      fprintf(stderr,"got packet header,size=%d,cmd=%X,id=%d\n",size,cmd,id);

      ppacket* inp = createpacket_r(size,cmd,id);
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

      mdscs_gotpacket(eptr,eptr->inpacket);
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

void mdscs_write(mdscsserventry *eptr) {
	int32_t i;

  while(eptr->outpacket){
		i=write(eptr->sock,eptr->outpacket->startptr,eptr->outpacket->bytesleft);

		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"mdscs module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
				eptr->mode = KILL;
			}
			return;
		}

    //debug
    fprintf(stderr,"wrote %d to (ip:%u.%u.%u.%u)\n",i,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

		eptr->outpacket->startptr += i;
		eptr->outpacket->bytesleft -=i;

    if(eptr->outpacket->bytesleft > 0) return;

    ppacket* p = eptr->outpacket;
    eptr->outpacket = eptr->outpacket->next;
    free(p);
	}
}

void mdscs_gotpacket(mdscsserventry* eptr,ppacket* p){
  switch(p->cmd){
    case CSTOMD_REGISTER:
      mdscs_register(eptr,p);
      break;
    case CSTOMD_UPDATE_STATUS:
      mdscs_update_status(eptr,p);
      break;
  }
}

mdscsserventry* mdscs_find_serventry(uint64_t chunkid){
  mdschunk* c = lookup_chunk(chunkid);
  if(c == NULL) return NULL;
  
  mdscsserventry* eptr = mdscsservhead;
  for(;eptr;eptr = eptr->next)
    if(eptr->peerip == c->csip)
      return eptr;

  return NULL;
}

void mdscs_new_chunk(mdschunk** c){
  mdscsserventry* eptr;

  for(eptr = mdscsservhead;eptr;eptr = eptr->next){
    if(eptr->mode == KILL) continue;

    if(eptr->availspace >= CHUNKSIZE){// not considering spatial load balancing among cs servers
      mdschunk* ret = new_chunk(global_chunk_counter++,eptr->peerip,0);
      ppacket *outp = createpacket_s(8,MDTOCS_CREATE,0);
      uint8_t* ptr = outp->startptr + HEADER_LEN;
      put64bit(&ptr,ret->chunkid);

      outp->next = eptr->outpacket;
      eptr->outpacket = outp; //just hope this packet gets to cs server first

      *c = ret;
      return;
    }
  }

  *c = NULL;
}

void mdscs_delete_chunk(uint64_t chunkid){
  mdscsserventry* eptr;
  mdschunk* c = lookup_chunk(chunkid);
  if(c == NULL) return;

  remove_chunk(chunkid);
  free_chunk(c);

  for(eptr = mdscsservhead;eptr;eptr = eptr->next){
    if(eptr->mode == KILL) continue;

    if(eptr->peerip == c->csip){
      ppacket* outp = createpacket_s(8,MDTOCS_DELETE,0);
      uint8_t* ptr = outp->startptr + HEADER_LEN;
      put64bit(&ptr,chunkid);

      outp->next = eptr->outpacket;
      eptr->outpacket = outp;

      return;
    }
  }
}

int mdscs_append_chunk(ppfile* f,mdschunk* c){
  int ret = file_append_chunk(f,c->chunkid);
  if(ret == 0 && f->chunks > 1){
    uint64_t previd = f->clist[f->chunks-2];
    mdscsserventry* ceptr = mdscs_find_serventry(previd);

    if(ceptr){
      ppacket* p = createpacket_s(8,MDTOCS_FILL_CHUNK,-1);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put64bit(&ptr,previd);

      p->next = ceptr->outpacket;
      ceptr->outpacket = p;
    } else {
      //@TODO
    }
  }
}

int mdscs_pop_chunk(ppfile* f,uint64_t* id){
  return file_pop_chunk(f,id);
}

void mdscs_register(mdscsserventry* eptr,ppacket* p){
  ppacket* outp;
  const uint8_t* ptr = p->startptr;
  int i;

  eptr->space = get32bit(&ptr);
  eptr->availspace = get32bit(&ptr);
  eptr->chunks = get32bit(&ptr);

  for(i=0;i<eptr->chunks;i++){
    uint64_t chunkid;
    int occupy;

    chunkid = get64bit(&ptr);
    occupy = get32bit(&ptr);

    mdschunk* c = new_chunk(chunkid,eptr->peerip,occupy);
    add_chunk(c);

    linklist* l = (linklist*)malloc(sizeof(linklist));
    l->next = eptr->clist;
    l->data = c;

    eptr->clist = l;

    if(c->chunkid > global_chunk_counter)
      global_chunk_counter = c->chunkid + 1;
  }

  outp = createpacket_s(4,MDTOCS_REGISTER,p->id);
  uint8_t* ptr2 = outp->startptr + HEADER_LEN;
  put32bit(&ptr2,0);

  outp->next = eptr->outpacket;
  eptr->outpacket = outp;
}

void mdscs_update_status(mdscsserventry* eptr,ppacket* p){
  ppacket* outp;
  const uint8_t* ptr = p->startptr;
  int i;

  eptr->space = get32bit(&ptr);
  eptr->availspace = get32bit(&ptr);
  eptr->chunks = get32bit(&ptr);
}
