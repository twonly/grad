#include "mdmd.h"
#include "mds_fs.h"
#include "datapack.h"

enum {KILL,HEADER,DATA};

static mdmdserventry* mdmdservhead = NULL;

static int lsock;
static int lsockpdescpos;

static pthread_t conn_thread;
static void* pcq_conn = NULL;
static void* pcq_fin = NULL;
static volatile int exiting;

static pthread_mutex_t conns_mutex;
static int conns;

int mdmd_init(void){
  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"mdmd: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);

  lsockpdescpos = -1;

  if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
    mfs_errlog_silent(LOG_NOTICE,"mdmd: can't set accept filter");
  }

	if (tcpstrlisten(lsock,"*",MDSMDS_PORT_STR,100)<0) { //listen to other mds
		mfs_errlog(LOG_ERR,"mdmd: can't listen on socket");
		return -1;
	}

  fprintf(stderr,"mdmd: listening on port %s\n",MDSMDS_PORT_STR);

  exiting = 0;
  conns = 0;
  pcq_fin = queue_new();
  pcq_conn = queue_new();
  pthread_mutex_init(&conns_mutex,NULL);
  pthread_create(&conn_thread,NULL,
                mdmd_conn_thread,NULL);

	main_destructregister(mdmd_term);
	main_pollregister(mdmd_desc,mdmd_serve);

  return 0;
}

void* mdmd_conn_thread(void* dum){
  (void)dum;

  while(!exiting){
    if(queue_isempty(pcq_conn)){
      sleep(10);
      continue;
    }

    uint32_t ip;
    char* data;
    if(queue_get(pcq_conn,&ip,&data) != 0){
      continue;
    }

    fprintf(stderr,"\n\n\n\n\nfinally!!!!!:%X\n\n\n\n",ip);

    int fd = tcpsocket();
    tcpnodelay(fd);

    if (tcpnumtoconnect(fd,ip,MDSMDS_PORT,CONN_TIMEOUT)<0) { //connect to mis, with timeout

      fprintf(stderr,"\n\n\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!cannot connect to %d\n\n\n",ip);
      tcpclose(fd);
      continue;
    }

    fprintf(stderr,"before that,queue_isempty:%d\n",queue_isempty(pcq_fin));
    fprintf(stderr,"putting %d to pcq_fin,result=%d\n",fd,queue_put(pcq_fin,fd,data));
    fprintf(stderr,"now queue_elements:%d\n",queue_elements(pcq_fin));
    fprintf(stderr,"noew queue_isempty:%d\n",queue_isempty(pcq_fin));
  }

  queue_delete(pcq_fin);
  queue_delete(pcq_conn);

  return NULL;
}

void mdmd_serve(struct pollfd *pdesc) {
	mdmdserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			fprintf(stderr,"mdmd: accept error\n");
		} else {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(mdmdserventry));
			passert(eptr);

			eptr->next = mdmdservhead;
			mdmdservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      eptr->type = 1;
      eptr->atime = time(NULL);
      memset(eptr->htab,0,sizeof(eptr->htab));

      fprintf(stderr,"another mds(ip:%u.%u.%u.%u) connected\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

      fflush(stderr);
		}
	}

  if(!queue_isempty(pcq_fin)){
    int ns;
    char* path;
    fprintf(stderr,"\n\n\n+fuck yeah\n\n\n");

    while(queue_get(pcq_fin,&ns,&path) == 0){
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(mdmdserventry));
			passert(eptr);

			eptr->next = mdmdservhead;
			mdmdservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

			tcpgetpeer(ns,&(eptr->peerip),NULL);
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      eptr->atime = time(NULL);
      eptr->type = 1;
      memset(eptr->htab,0,sizeof(eptr->htab));
      mdmdserventry_add_path(eptr,path);
      free(path);

      fprintf(stderr,"connected to another mds(ip:%u.%u.%u.%u)\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

      fflush(stderr);
    }
  }

// read
	for (eptr=mdmdservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				mdmd_read(eptr);
			}
		}
	}

// write
	for (eptr=mdmdservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if ((((pdesc[eptr->pdescpos].events & POLLOUT)==0 && (eptr->outpacket!=NULL)) || (pdesc[eptr->pdescpos].revents & POLLOUT)) && eptr->mode!=KILL) {
				mdmd_write(eptr);
			}
		}
	}

  mdmdserventry** kptr = &mdmdservhead;
  while ((eptr=*kptr)) {
    if (eptr->mode == KILL) {
      tcpclose(eptr->sock);

      if(eptr->type == 2){
        pthread_mutex_lock(&conns_mutex);
        conns--;
        pthread_mutex_unlock(&conns_mutex);
      }

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
      mdmdserventry_free(eptr);
    } else {
      kptr = &(eptr->next);
    }
  }
}

void mdmd_desc(struct pollfd *pdesc,uint32_t *ndesc) {
	uint32_t pos = *ndesc;
	mdmdserventry *eptr;

  pdesc[pos].fd = lsock;
  pdesc[pos].events = POLLIN;
  lsockpdescpos = pos;
  pos++;

	for(eptr=mdmdservhead ; eptr ; eptr=eptr->next){
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

void mdmd_term(void) {
	mdmdserventry *eptr,*eptrn;

	fprintf(stderr,"mdmd: closing %s:%s\n","*",MDSMDS_PORT_STR);
	tcpclose(lsock);

	for (eptr = mdmdservhead ; eptr ; eptr = eptrn) {
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

		mdmdserventry_free(eptr);
	}

  exiting = 1;
}

void mdmd_write(mdmdserventry *eptr){
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

void mdmd_read(mdmdserventry *eptr){
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

      fprintf(stderr,"got packet header,size=%d,cmd=%X,id=(%u.%u.%u.%u),bytesleft=%d\n",size,cmd,(id>>24)&0xFF,(id>>16)&0xFF,(id>>8)&0xFF,id&0xFF,inp->bytesleft);
      continue;
    } else {
      eptr->inpacket->bytesleft -= i;
      eptr->inpacket->startptr += i;

      if(eptr->inpacket->bytesleft > 0) return;

      eptr->inpacket->startptr = eptr->inpacket->buf;

      fprintf(stderr,"packet received,size=%d,cmd=%X,id=%d\n",eptr->inpacket->size,eptr->inpacket->cmd,eptr->inpacket->id);

      mdmd_gotpacket(eptr,eptr->inpacket);
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

void mdmd_add_path(uint32_t ip,char* path){
  fprintf(stderr,"\n\n\n+mdmd_add_path:%X,%s\n\n\n",ip,path);

  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    if(eptr->peerip == ip) break;

    eptr = eptr->next;
  }

  if(eptr != NULL){
    mdmdserventry_add_path(eptr,path);
    return;
  }

  if(pthread_mutex_trylock(&conns_mutex) != 0){
    return;
  }
  if(conns >= MAX_MDS_CONN){
    mdmdserventry* eptr,*iter;

    eptr = NULL;
    iter = mdmdservhead;
    while(iter){
      if(iter->type== 1) continue; //ignore incoming connection
      if(eptr == NULL ||
         eptr->atime > iter->atime){ //@TODO:maybe a better replacement policy?
        eptr = iter;
      }

      iter = iter->next;
    }

    if(eptr){
      eptr->mode = KILL;
      eptr->type = -1; //avoid duplicate removal when closing KILL mode entries

      conns--;
    }
    else{
      return;
    }
  }

  conns++;
  pthread_mutex_unlock(&conns_mutex);

  queue_put(pcq_conn,ip,strdup(path));
}

mdmdserventry* mdmd_find_link(char* path){
  fprintf(stderr,"+mdmd_find_link:path=%s\n",path);
  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    fprintf(stderr,"eptr=%X\n",eptr->peerip);

    if(mdmdserventry_has_path(eptr,path)){
      return eptr;
    }

    eptr = eptr->next;
  }

  return NULL;
}

void mdmd_gotpacket(mdmdserventry* eptr,ppacket* p){
  switch(p->cmd){
    case MDTOMD_S2C_READ_CHUNK_INFO:
      mdmd_s2c_read_chunk_info(eptr,p);
      break;
    case MDTOMD_C2S_READ_CHUNK_INFO:
      mdmd_c2s_read_chunk_info(eptr,p);
      break;
  }
}

void mdmd_read_chunk_info(mdmdserventry* eptr,char* path,int id){
  fprintf(stderr,"+mdmd_read_chunk_info\n");

  int plen = strlen(path);
  ppacket* p = createpacket_s(4+plen,MDTOMD_C2S_READ_CHUNK_INFO,id);
  uint8_t* ptr = p->startptr + HEADER_LEN;

  fprintf(stderr,"+path:%s,plen=%d\n",path,plen);

  put32bit(&ptr,plen);
  memcpy(ptr,path,plen);
  ptr += plen;

  p->next = eptr->outpacket;
  eptr->outpacket = p;

  eptr->atime = time(NULL);
}

void mdmd_s2c_read_chunk_info(mdmdserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mdmd_s2c_read_chunk_info\n");
  mdsserventry* mds_eptr = mds_entry_from_id(inp->id);
  if(mds_eptr){
    const uint8_t* inptr = inp->startptr;
    int status = get32bit(&inptr);
    ppacket* p = NULL;

    if(status != 0){
      p = createpacket_r(4,MITOMD_READ_CHUNK_INFO,inp->id);
      uint8_t* ptr = p->startptr;
      put32bit(&ptr,status);
    } else {
      p = createpacket_r(inp->size+4,MITOMD_READ_CHUNK_INFO,inp->id);
      uint8_t* ptr = p->startptr;
      put32bit(&ptr,0);
      put32bit(&ptr,eptr->peerip);
      memcpy(ptr,inptr,inp->size-4);
      ptr += inp->size - 4;
    }

    mds_fw_read_chunk_info(mds_eptr,p);
    free(p);
  }
}

void mdmd_c2s_read_chunk_info(mdmdserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mdmd_c2s_read_chunk_info\n");

  int plen,mdsid,i;
  const uint8_t* ptr = inp->startptr;
  ppacket* outp = NULL;

  char* path = (char*)malloc(plen+10);
  plen = get32bit(&ptr);
  memcpy(path,ptr,plen);
  ptr += plen;

  path[plen] = 0;
  fprintf(stderr,"plen=%d,path=%s\n",plen,path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    outp = createpacket_s(4,MDTOMD_S2C_READ_CHUNK_INFO,inp->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    int totsize = 4+4+8*(f->chunks);

    outp = createpacket_s(totsize,MDTOMD_S2C_READ_CHUNK_INFO,inp->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;
    put32bit(&ptr,0);

    //no address here
    put32bit(&ptr,f->chunks);
    for(i=0;i<f->chunks;i++){
      put64bit(&ptr,f->clist[i]);
    }

    fprintf(stderr,"chunks=%d\n",f->chunks);
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
}

void mdmdserventry_add_path(mdmdserventry* eptr,char* path){
  int k = strhash(path) % MDMD_HASHSIZE;
  if(eptr->htab[k] == NULL){
    eptr->htab[k] = malloc(sizeof(hashnode));
    eptr->htab[k]->key = strdup(path);
    eptr->htab[k]->next = NULL;
  } else if(eptr->htab[k]->next == NULL){ //only allow two paths in one slot
    if(!strcmp(eptr->htab[k]->key,path)){ //already in hash
      return;
    }
    hashnode* n = malloc(sizeof(hashnode));
    n->key = strdup(path);
    n->next = NULL;
    eptr->htab[k]->next = n;
  }
}

int mdmdserventry_has_path(mdmdserventry* eptr,char* path){
  int k = strhash(path) % MDMD_HASHSIZE;
  hashnode* n = eptr->htab[k];
  while(n){
    if(!strcmp(n->key,path)){
      return 1;
    }

    n = n->next;
  }

  return 0;
}

void mdmdserventry_free(mdmdserventry* eptr){
  int i;
  for(i=0;i<MDMD_HASHSIZE;i++){
    if(eptr->htab[i]){
      if(eptr->htab[i]->next){
        free(eptr->htab[i]->next->key);
        free(eptr->htab[i]->next);
      }
      free(eptr->htab[i]->key);
      free(eptr->htab[i]);
    }
  }

  free(eptr);
}

