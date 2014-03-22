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

static int conns;

#define STAT_QUERY 0x100
#define STAT_PCACHE_HIT 0x101
#define STAT_DCACHE_HIT 0x102
#define STAT_MISS 0x103
#define STAT_EVENTUAL_MISS 0x104

#define STAT_QUERY_STR "stat_query"
#define STAT_PCACHE_HIT_STR "stat_pcache_h"
#define STAT_DCACHE_HIT_STR "stat_dcache_h"
#define STAT_MISS_STR "stat_miss"
#define STAT_EVENTUAL_MISS_STR "stat_eventual_miss"

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
  pthread_create(&conn_thread,NULL,
                mdmd_conn_thread,NULL);

	main_destructregister(mdmd_term);
	main_pollregister(mdmd_desc,mdmd_serve);
  main_timeregister(TIMEMODE_RUN_LATE,MDMD_PATH_EXPIRE,0,mdmdserventry_purge_cache);
  main_timeregister(TIMEMODE_RUN_LATE,MDMD_DECAY_TIME,0,mdmd_heuristic_decay);

  mdmd_stat_add_entry(STAT_QUERY,STAT_QUERY_STR,0);
  mdmd_stat_add_entry(STAT_PCACHE_HIT,STAT_PCACHE_HIT_STR,0);
  mdmd_stat_add_entry(STAT_DCACHE_HIT,STAT_DCACHE_HIT_STR,0);
  mdmd_stat_add_entry(STAT_MISS,STAT_MISS_STR,0);
  mdmd_stat_add_entry(STAT_EVENTUAL_MISS,STAT_EVENTUAL_MISS_STR,0);

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
    if(queue_get(pcq_conn,&ip,(void**)&data) != 0){
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

//void* mdmd_connect(uint32_t ip){
//    uint32_t ip;
//
//    int fd = tcpsocket();
//    tcpnodelay(fd);
//
//    if (tcpnumtoconnect(fd,ip,MDSMDS_PORT,CONN_TIMEOUT)<0) { //connect to mis, with timeout
//      fprintf(stderr,"\n\n\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!cannot connect to %d\n\n\n",ip);
//      tcpclose(fd);
//      continue;
//    }
//    fprintf(stderr,"noew queue_isempty:%d\n",queue_isempty(pcq_fin));
//  return NULL;
//}

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
      eptr->atime = main_time();
      memset(eptr->htab,0,sizeof(eptr->htab));

      fprintf(stderr,"another mds(ip:%u.%u.%u.%u) connected\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

      fflush(stderr);
		}
	}

  if(!queue_isempty(pcq_fin)){ //why put it here
    int ns;
    mdmd_path_st* mps;
    fprintf(stderr,"\n\n\n+fuck yeah\n\n\n");

    while(queue_get(pcq_fin,&ns,(void**)&mps) == 0){
      fprintf(stderr,"ns=%d\n",ns);
      uint32_t ip;
      tcpgetpeer(ns,&ip,NULL); //ns is fd

      eptr = mdmdserventry_from_ip(ip);
      if(eptr != NULL){
        mdmd_create_access_entry(eptr,mps->path,mps->type);
        free(mps);

        tcpclose(ns); //close redundant connections caused by thread sync problems
        conns--;
        continue;
      }

			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = malloc(sizeof(mdmdserventry));
			passert(eptr);

			eptr->next = mdmdservhead;
			mdmdservhead = eptr;

			eptr->sock = ns;
			eptr->pdescpos = -1;

      eptr->peerip = ip;
			eptr->mode = HEADER;

      eptr->inpacket = NULL;
      eptr->outpacket = NULL;
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      eptr->atime = main_time();
      eptr->type = 1;
      memset(eptr->htab,0,sizeof(eptr->htab));
      mdmdserventry_add_entry(eptr,mps);

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
        conns--;
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

void mdmd_create_access_entry(mdmdserventry* eptr,char* path,int type){
  int k = strhash(path) % (MDMD_HASHSIZE/2);
  if(type != MDMD_PATH_CACHE){
    k += (MDMD_HASHSIZE/2);
  }

  hashnode* n = eptr->htab[k];

  while(n){
    if(!strcmp(n->key,path)){ //access
      mdmd_path_st* mps = n->data;
      mps->visit++;
      mps->atime = main_time();

      return;
    }

    n = n->next;
  }

  mdmd_path_st* mps = malloc(sizeof(mdmd_path_st));
  mps->path = strdup(path);
  mps->ip = eptr->peerip;
  mps->type = type;
  mps->visit = 1;
  mps->ctime = mps->atime = main_time();
  mdmdserventry_add_entry(eptr,mps);
}

void mdmd_add_entry(uint32_t ip,char* path,int type){
  fprintf(stderr,"\n\n\n+mdmd_add_entry:%X,%s,%d\n\n\n",ip,path,type);

  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    if(eptr->peerip == ip) break;

    eptr = eptr->next;
  }

  if(eptr != NULL){ //connection already exists
    mdmd_create_access_entry(eptr,path,type); //create or update an visit info entry
    return;
  }
    
  if(conns >= MAX_MDS_CONN){ //not exist, remove some connections if the number exceeds the MAX 
    mdmdserventry* eptr,*iter;

    eptr = NULL;
    iter = mdmdservhead;
    while(iter){
      if(iter->type== 1) continue; //ignore incoming connection
      if(eptr == NULL ||
         eptr->atime > iter->atime){ //@TODO:maybe a better replacement policy?
        eptr = iter; //find the oldest eptr
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
    int fd = tcpsocket();
    tcpnodelay(fd);

    if (tcpnumtoconnect(fd,ip,MDSMDS_PORT,CONN_TIMEOUT)<0) { //connect to mis, with timeout

      fprintf(stderr,"\n\n\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!cannot connect to %d\n\n\n",ip);
      tcpclose(fd);
    } else {
      fprintf(stderr,"\n\n\n\n\n!!!!!!!!!!!!!!!!!!!!!!!!!!connect to Primary MDS %X\n\n\n",ip);
        mdmdserventry* neptr = (mdmdserventry*)malloc(sizeof(mdmdserventry));
        neptr->next = mdmdservhead;
        mdmdservhead = neptr;

        neptr->sock = fd;
        neptr->pdescpos = -1;

        neptr->peerip = ip;
        neptr->mode = HEADER;

        neptr->inpacket = NULL;
        neptr->outpacket = NULL;
        neptr->bytesleft = HEADER_LEN;
        neptr->startptr = neptr->headbuf;

        neptr->type = 1;
        neptr->atime = main_time();
        memset(neptr->htab,0,sizeof(neptr->htab));
    }
    return;

  mdmd_path_st* mps = malloc(sizeof(mdmd_path_st));
  mps->path = strdup(path);
  mps->ip = ip;
  mps->type = type;
  mps->visit = 1;
  mps->atime = mps->ctime = main_time();

  fprintf(stderr,"queue_put:%X,%s\n",ip,mps->path);
  queue_put(pcq_conn,ip,mps); //use to connect to new Replica MDS
}

mdmdserventry* mdmd_find_link(char* path){
  fprintf(stderr,"+mdmd_find_link:path=%s\n",path);

  if(!strcmp(path,"/")){//special case
    return NULL;
  }

  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    fprintf(stderr,"eptr=%X\n",eptr->peerip);

    if(mdmdserventry_has_path(eptr,path)){ //should update the visit table
        //mdmd_path_st* mps = malloc(sizeof(mdmd_path_st));
        //mps->path = strdup(path);
        //mps->type = type;
        //mps->visit = 1;
        //mdmdserventry_add_entry(eptr, mps);
      mdmd_stat_count(STAT_PCACHE_HIT);
      return eptr;
    }

    eptr = eptr->next;
  }

  char* dir = parentdir(path);
  eptr = mdmd_find_dir(dir);
  free(dir);
  if(eptr != NULL){
    mdmd_stat_count(STAT_DCACHE_HIT);
  } else {
    mdmd_stat_count(STAT_MISS);
  }

  return eptr;
}

mdmdserventry* mdmd_find_dir(char* dir){
  fprintf(stderr,"+mdmd_find_dir:dir=%s\n",dir);
  int totvisit = 0;
  uint32_t now = main_time();
  int totdifftime = 0;
  double srt = 0.0;

  //compute totvisit & totdifftime
  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    mdmd_path_st* n = mdmdserventry_find_dir(eptr,dir);
    if(n){
      totvisit += n->visit;

      int delta = now - n->atime;
      if(delta <= 0) delta = 1;
      totdifftime += delta;
    }

    eptr = eptr->next;
  }

  //compute srt
  eptr = mdmdservhead;
  while(eptr){
    mdmd_path_st* n = mdmdserventry_find_dir(eptr,dir);
    if(n){
      int delta = now - n->atime;
      if(delta <= 0) delta = 1;
      srt += totdifftime / (double)delta;
    }

    eptr = eptr->next;
  }

  //finds the entry having max Q
  mdmdserventry* select = NULL;
  double bestQ = -1;
  uint32_t bestatime = -1;
  eptr = mdmdservhead;
  while(eptr){
    mdmd_path_st* n = mdmdserventry_find_dir(eptr,dir);
    if(n){
      double Q = 0.0;
      Q += (MDMD_FREQ_FACTOR)* n->visit / (double)totvisit;

      int delta = now - n->atime;
      if(delta <= 0) delta = 1;
      Q += (MDMD_TIME_FACTOR)* (totdifftime / delta) / srt;

      if(select == NULL || Q > bestQ ||
          (Q == bestQ && n->atime > bestatime)){
        select = eptr;
        bestQ = Q;
        bestatime = n->atime;
      }
    }

    eptr = eptr->next;
  }

  return select;
}

void mdmd_gotpacket(mdmdserventry* eptr,ppacket* p){
  switch(p->cmd){
    case MDTOMD_SEND_ATTR:
        mdmd_get_attr(eptr, p);
        break;
    case MDTOMD_UPDATE_ATTR:
        mdmd_get_attr(eptr, p);
        break;
    case MDTOMD_S2C_READ_CHUNK_INFO:
      mdmd_s2c_read_chunk_info(eptr,p);
      break;
    case MDTOMD_C2S_READ_CHUNK_INFO:
      mdmd_c2s_read_chunk_info(eptr,p);
      break;
    case MDTOMD_S2C_GETATTR:
      mdmd_s2c_getattr(eptr,p);
      break;
    case MDTOMD_C2S_GETATTR:
      mdmd_c2s_getattr(eptr,p);
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

  eptr->atime = main_time();
}

void mdmd_s2c_read_chunk_info(mdmdserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mdmd_s2c_read_chunk_info\n");
  mdsserventry* mds_eptr = mds_entry_from_id(inp->id);

  const uint8_t* inptr = inp->startptr;
  int plen = get32bit(&inptr);
  char* path = malloc(plen+10);
  memcpy(path,inptr,plen);
  inptr += plen;
  path[plen] = 0;

  //rest: inp->size - 4 - plen
  int status = get32bit(&inptr);
  ppacket* p = NULL;

  if(mds_eptr){
    if(status != 0){ //forward to mis
      p = createpacket_r(4+plen,MDTOMI_READ_CHUNK_INFO,inp->id);
      uint8_t* ptr = p->startptr;
      put32bit(&ptr,plen);
      memcpy(ptr,path,plen);
      ptr += plen;

      mds_direct_pass_mi(p,MDTOMI_READ_CHUNK_INFO);
      
      mdmd_stat_count(STAT_EVENTUAL_MISS);
    } else {
      p = createpacket_r(inp->size-4-plen+4,MITOMD_READ_CHUNK_INFO,inp->id);
      uint8_t* ptr = p->startptr;
      put32bit(&ptr,0);
      put32bit(&ptr,eptr->peerip);
      memcpy(ptr,inptr,inp->size-8-plen);

      mds_fw_read_chunk_info(mds_eptr,p);

      //cache hit
    }

  } else {
    if(status!=0){
      mdmd_stat_count(STAT_EVENTUAL_MISS);
    }
    else{
      //cache hit
    }
  }

  free(p);
  free(path);
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
    outp = createpacket_s(4+plen+4,MDTOMD_S2C_READ_CHUNK_INFO,inp->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;

    put32bit(&ptr,plen);
    memcpy(ptr,path,plen);
    ptr += plen;
    put32bit(&ptr,-ENOENT);
  } else {
    int totsize = 4+plen+4+4+8*(f->chunks);

    outp = createpacket_s(totsize,MDTOMD_S2C_READ_CHUNK_INFO,inp->id);
    uint8_t* ptr = outp->startptr + HEADER_LEN;

    put32bit(&ptr,plen);
    memcpy(ptr,path,plen);
    ptr += plen;
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

int mdmd_path_st_hash(mdmd_path_st* mps){
  int k = strhash(mps->path) % (MDMD_HASHSIZE/2);
  if(mps->type == MDMD_DIR_HEURISTIC){
    k += (MDMD_HASHSIZE/2);
  }

  return k;
}

void mdmdserventry_add_entry(mdmdserventry* eptr,mdmd_path_st* mps){
  int k = mdmd_path_st_hash(mps);

  hashnode* n = malloc(sizeof(hashnode));
  n->key = mps->path;
  n->data = mps;

  n->next = eptr->htab[k];
  eptr->htab[k] = n;
}

int mdmdserventry_has_path(mdmdserventry* eptr,char* path){
  int k = strhash(path) % (MDMD_HASHSIZE/2);
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
    hashnode *n,*nn;
    nn = NULL;
    n = eptr->htab[i];

    while(n){
      nn = n->next;

      mdmd_path_st* mps = n->data;
      free(mps->path);
      free(mps);

      free(n);

      n = nn;
    }
  }

  free(eptr);
}

mdmd_path_st* mdmdserventry_find_dir(mdmdserventry* eptr,char* dir){
  int k = strhash(dir) % (MDMD_HASHSIZE/2) + (MDMD_HASHSIZE/2);
  hashnode* n = eptr->htab[k];

  while(n){
    if(!strcmp(n->key,dir)){
      return n->data;
    }

    n = n->next;
  }

  return NULL;
}

mdmdserventry* mdmdserventry_from_ip(uint32_t ip){
  mdmdserventry* eptr = mdmdservhead;
  while(eptr){
    if(eptr->peerip == ip){
      return eptr;
    }

    eptr = eptr->next;
  }

  return NULL;
}

void mdmd_heuristic_decay(void) {
  mdmdserventry *eptr = mdmdservhead;
  while (eptr) {
      int i;
      for(i=0;i<MDMD_HASHSIZE/2;i++) {
          hashnode * n = eptr->htab[i];
          while(n) {
              mdmd_path_st * mps = n->data;
              mps->history = mps->history/2+mps->visit;
              mps->visit = 0;
              syslog(LOG_WARNING, "path: %s, visit:%d, history:%d", mps->path, mps->visit, mps->history);
              n = n->next;
          }
      }
      eptr = eptr->next;
  }
  return;
}

void mdmdserventry_purge_cache(void){
  mdmdserventry* eptr = mdmdservhead;
  uint32_t now = main_time();

  while(eptr){
    int i;
    for(i=0;i<MDMD_HASHSIZE;i++){
      hashnode* n = eptr->htab[i];
      hashnode* pn = NULL;

      while(n){
        mdmd_path_st* mps = n->data;

        if(now - mps->ctime > MDMD_PATH_EXPIRE){ //not tested
          if(pn == NULL){
            eptr->htab[i] = n->next;
          } else {
            pn->next = n->next;
          }

          free(mps->path);
          free(mps);
          free(n);

          if(pn == NULL){
            n = eptr->htab[i];
          } else {
            n = pn->next;
          }
        } else {
          pn = n;
          n = n->next;
        }
      }

    }
    eptr = eptr->next;
  }
}

void mdmd_send_attr( int repip, ppfile* f) { //Primary MDS send attr to Replica when first created
  fprintf(stderr,"+mdmd_send_attr\n");
  mdmdserventry *meptr = mdmdserventry_from_ip(repip);
    if(!meptr) {
        fprintf(stderr, "can not find mdmd eptr from repip");
        return;
    }
    fprintf(stderr, "find mdmd eptr from repip\n");
  int plen = strlen(f->path);
    fprintf(stderr, "f->path len: %d\n", plen);
  ppacket* p = createpacket_s(4+plen+sizeof(attr),MDTOMD_SEND_ATTR,-1);
  uint8_t* ptr = p->startptr + HEADER_LEN;
  fprintf(stderr,"+path:%s,plen=%d\n",f->path,plen);
  put32bit(&ptr,plen);
  memcpy(ptr,f->path,plen);
  ptr += plen;
  memcpy(ptr, &(f->a), sizeof(attr));

  p->next = meptr->outpacket;
  meptr->outpacket = p;
}

void mdmd_get_attr(mdmdserventry* eptr, ppacket* inp) { //replica MDS receive attr, handle both CREATE_REPLICA and UPDATE_ATTR
  fprintf(stderr,"+mdmd_get_attr\n");
  //mdsserventry* mds_eptr = mds_entry_from_id(inp->id);

  const uint8_t* inptr = inp->startptr;
  int plen = get32bit(&inptr);
  char* path = malloc(plen+10);
  memcpy(path,inptr,plen);
  inptr += plen;
  path[plen] = 0;
  attr a;
  ppfile* f = lookup_file(path);
  if(f) { //file exist, just update attr
    fprintf(stderr, "file exist, update attr");
    memcpy(&a,inptr,sizeof(attr));
    f->a = a;
  } else { //file not exist, create new file
    fprintf(stderr, "file not exist, create file in replica");
    memcpy(&a, inptr, sizeof(attr));
    ppfile* nf = new_file(path, a);
    nf->srcip = eptr->peerip; //primary ip, may be useful
    nf->rep_list = NULL; //later should record visit info
    nf->rep_cnt = 0;
    add_file(nf); //add to hash list
  }
  
  //rest: inp->size - 4 - plen
  //int status = get32bit(&inptr);
  //ppacket* p = NULL;

}

void mdmd_update_attr(int repip, ppfile* f){
  fprintf(stderr,"+mdmd_update_attr\n");
    mdmdserventry* meptr = mdmdserventry_from_ip(repip);
    if(!meptr) {
        fprintf(stderr, "can not find mdmd eptr from repip");
        return;
    }
    
  int plen = strlen(f->path);
  ppacket* outp = createpacket_s(4+plen+sizeof(attr),MDTOMD_UPDATE_ATTR,-1);
  uint8_t* ptr2 = outp->startptr + HEADER_LEN;

  put32bit(&ptr2,plen);

  memcpy(ptr2,f->path,plen);
  ptr2 += plen;

  memcpy(ptr2,&f->a,sizeof(attr));

  outp->next = meptr->outpacket;
  meptr->outpacket = outp;
}

void mdmd_getattr(mdmdserventry* eptr,char* path,int id){ //triggered by MDS
  fprintf(stderr,"+mdmd_getattr\n");

  int plen = strlen(path);
  ppacket* p = createpacket_s(4+plen,MDTOMD_C2S_GETATTR,id);
  uint8_t* ptr = p->startptr + HEADER_LEN;

  fprintf(stderr,"+path:%s,plen=%d\n",path,plen);

  put32bit(&ptr,plen);
  memcpy(ptr,path,plen);
  ptr += plen;

  p->next = eptr->outpacket;
  eptr->outpacket = p;

  eptr->atime = main_time();
}

void mdmd_s2c_getattr(mdmdserventry* eptr,ppacket* inp){ //receive from Primary MDS
  fprintf(stderr,"+mdmd_s2c_getattr!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
  mdsserventry* mds_eptr = mds_entry_from_id(inp->id);

  const uint8_t* inptr = inp->startptr;
  int status = get32bit(&inptr);
  int plen = get32bit(&inptr);
  char* path = (char*)malloc(plen+1);
  memcpy(path,inptr,plen);
  path[plen] = 0;
  inptr += plen;
  fprintf(stderr,"status is %d\n", status);

  //rest: inp->size - 4 - plen
  ppacket* p = NULL;

  if(status != 0){ //return errno to Client
      fprintf(stderr,"Not found in Primary, fail\n");
      p = createpacket_s(4,MDTOCL_GETATTR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,status);
      mds_direct_pass_cl(eptr,p,MDTOCL_GETATTR);
  } else {
      fprintf(stderr,"Found in Primary, success\n");
      p = createpacket_s(4+sizeof(attr),MDTOCL_GETATTR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,0);
      fprintf(stderr,"sizeof(attr) is %d\n", sizeof(attr));
      memcpy(ptr,inptr,sizeof(attr));

      //can not use mds_direct_pass_cl function directly. Format not matched cuz not processed by mds_read().
      mdsserventry* ceptr = mds_entry_from_id(p->id);
      fprintf(stderr, "mds_direct_pass_cl entry id is %X\n", p->id);
      if(ceptr){
          ppacket* outp = createpacket_s(p->size,MDTOCL_GETATTR,p->id);
          memcpy(outp->startptr+HEADER_LEN,p->startptr+HEADER_LEN,p->size);

          outp->next = ceptr->outpacket;
          ceptr->outpacket = outp;
      }
      //mds_direct_pass_cl(eptr,p,MDTOCL_GETATTR);
      //TODO
      //Update visit info
  }
  free(path);
}

void mdmd_c2s_getattr(mdmdserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mdmd_c2s_getattr!!!!!!!!!!!!!!!!!!\n");

  int plen,mdsid,i;
  const uint8_t* ptr = inp->startptr;
  ppacket* outp = NULL;

  char* path = (char*)malloc(plen+1);
  plen = get32bit(&ptr);
  memcpy(path,ptr,plen);
  path[plen] = 0;
  ptr += plen;

  fprintf(stderr,"plen=%d,path=%s   ",plen,path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    fprintf(stderr,"not found!!!!!!!!!!!!!!!!!!\n");
    outp = createpacket_s(4+plen+4,MDTOMD_S2C_GETATTR,inp->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;

    put32bit(&ptr2,-ENOENT);
    put32bit(&ptr2,plen);
    memcpy(ptr2,path,plen);
    ptr2 += plen;
  } else {
    fprintf(stderr,"found!!!!!!!!!!!!!!!!!1\n");
    int totsize = 4+plen + 4 + sizeof(attr);
    outp = createpacket_s(totsize,MDTOMD_S2C_GETATTR,inp->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;
    put32bit(&ptr2,0);//status
    put32bit(&ptr2,plen);
    memcpy(ptr2,path,plen);
    ptr2 += plen;
    memcpy(ptr2,&f->a,sizeof(attr));
  }

  outp->next = eptr->outpacket;
  eptr->outpacket = outp;
  free(path);
}
