#include "echo.h"

echoserventry* echoservhead = NULL;

int lsock;
int lsockpdescpos;

int echo_init(void){
  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"main master server module: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);
  //nothing

  lsockpdescpos = -1;

	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"main master server module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,"*","8080",100)<0) {
		mfs_errlog(LOG_ERR,"main master server module: can't listen on socket");
		return -1;
	}
  
  fprintf(stderr,"listening on port 8080\n");

	main_destructregister(echo_term);
	main_pollregister(echo_desc,echo_serve);

  return 0;
}

void echo_serve(struct pollfd *pdesc) {
	echoserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"main master server module: accept error");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(echoserventry));
			passert(eptr);

			eptr->next = echoservhead;
			echoservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = DATA;
		}
	}

// read
	for (eptr=echoservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				echo_read(eptr);
			}
		}
	}

// write
	for (eptr=echoservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->writelen!=0)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				echo_write(eptr);
			}
		}
	}

	echoserventry** kptr = &echoservhead;
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

void echo_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	echoserventry *eptr;

  pdesc[pos].fd = lsock;
  pdesc[pos].events = POLLIN;
  lsockpdescpos = pos;
  pos++;

	for(eptr=echoservhead ; eptr ; eptr=eptr->next){
		pdesc[pos].fd = eptr->sock;
		pdesc[pos].events = 0;
		eptr->pdescpos = pos;

		pdesc[pos].events |= POLLIN;
		if (eptr->writelen != 0) {
			pdesc[pos].events |= POLLOUT;
		}
		pos++;
	}
	*ndesc = pos;
}

void echo_term(void) {
	echoserventry *eptr,*eptrn;

	syslog(LOG_NOTICE,"main master server module: closing %s:%s","*",8080);
	tcpclose(lsock);

	for (eptr = echoservhead ; eptr ; eptr = eptrn) {
		eptrn = eptr->next;
		free(eptr);
	}
}

void echo_read(echoserventry *eptr) {
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;
  char* buf = eptr->buffer;
  int len = sizeof(eptr->buffer);
  int writelen = 0;

	for (;;) {
		i=read(eptr->sock,buf,len);
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

    buf += i;
    len -= i;
    writelen += i;

		if (*(buf-1) == '\n') { //delimiter
      fprintf(stderr,"read %d from (ip:%u.%u.%u.%u)\n",writelen,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
      eptr->writelen = writelen;
			return;
		}
	}
}

void echo_write(echoserventry *eptr) {
	int32_t i;
  char* buf = eptr->buffer;
  int len = eptr->writelen;

	for (;;) {
		i=write(eptr->sock,buf,len);
		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"main master server module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
        eptr->writelen = 0;
				eptr->mode = KILL;
			}
			return;
		}

		buf += i;
		len -=i;

		if (len <= 0){
      fprintf(stderr,"wrote %d from (ip:%u.%u.%u.%u):%s\n",eptr->writelen,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
      eptr->writelen = 0;
			return;
		}
	}
}

