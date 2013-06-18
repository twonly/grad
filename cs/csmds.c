#include "csmds.h"
#include "datapack.h"

csmdsserventry* csmds = NULL;

char* mdshostip = "127.0.0.1";

int csmds_init(void){
  int mdsip,mdsport;
  int msock;

  inet_pton(AF_INET,mdshostip,&mdsip);
  mdsip = htonl(mdsip);
  mdsport = MDS_PORT;

  msock = tcpsocket();
  if (tcpnodelay(msock)<0) {
    fprintf(stderr,"can't set TCP_NODELAY\n");
  }

  fprintf(stderr,"connecting to %u.%u.%u.%u:%d\n",(mdsip>>24)&0xFF,(mdsip>>16)&0xFF,(mdsip>>8)&0xFF,mdsip&0xFF,mdsport);
  if (tcpnumconnect(msock,mdsip,mdsport)<0) {
    fprintf(stderr,"can't connect to mds (\"%X\":\"%"PRIu16"\")\n",mdsip,mdsport);
    tcpclose(msock);
    return -1;
  }

	csmdsserventry* eptr = malloc(sizeof(csmdsserventry));
	passert(eptr);

	eptr->sock = msock;
	eptr->pdescpos = -1;

  eptr->peerip = mdsip;
  eptr->mode = HEADER;

  eptr->inpacket = NULL;
  eptr->outpacket = NULL;
  eptr->bytesleft = HEADER_LEN;
  eptr->startptr = eptr->headbuf;

	main_destructregister(csmds_term);
	main_pollregister(csmds_desc,csmds_serve);

  return 0;
}

void csmds_serve(struct pollfd *pdesc) {
	csmdsserventry *eptr;

  eptr = csmds;
  if (eptr->pdescpos>=0) {
    if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
      eptr->mode = KILL;
    }
    if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
      csmds_read(eptr);
    }
  }

  eptr = csmds;
  if (eptr->pdescpos>=0) {
    if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
      csmds_write(eptr);
    }
  }

  if(csmds->mode == KILL){//Oops
    fprintf(stderr,"connection to mds closed\n");
    //maybe reconnect?
    kill(getpid(),SIGINT); //interrupt
  }
}

void csmds_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	csmdsserventry *eptr;

  eptr = csmds;
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

void csmds_term(void) {
	csmdsserventry *eptr;

  tcpclose(csmds->sock);

  eptr = csmds;

  ppacket *pp,*ppn;
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

void csmds_read(csmdsserventry *eptr) {
	int i;
  int size,cmd,id;

	while(1){
    if(eptr->mode == HEADER){
		  i=read(eptr->sock,eptr->startptr,eptr->bytesleft);
    } else {
      i=read(eptr->sock,eptr->inpacket->startptr,eptr->inpacket->bytesleft);
    }

		if (i==0) {
      fprintf(stderr,"connection with mds(ip:%u.%u.%u.%u) has been closed by peer\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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
      id = eptr->peerip;

      get32bit(&pptr); //discarded

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

      csmds_gotpacket(eptr,eptr->inpacket);
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

void csmds_write(csmdsserventry *eptr) {
	int32_t i;

  while(eptr->outpacket){
		i=write(eptr->sock,eptr->outpacket->startptr,eptr->outpacket->bytesleft);

		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"csmds: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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
