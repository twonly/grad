#include "mds.h"
#include "datapack.h"
#include "mds_fs.h"

enum {KILL,HEADER,DATA};

static mdsserventry* mdsservhead = NULL;

static int lsock;
static int lsockpdescpos;

mdsserventry* mdtomi = NULL;

int max(int a,int b){
  return a>b?a:b;
}

#define STAT_DIR_MISS 0x2013
#define STAT_DIR_MISS_STR "stat_dir_miss"

int mds_init(void){
  int misip,misport;
  int msock;
  create_count = 0;
  delete_count = 0;
  total_create = 0;
  total_delete = 0;
  local_hit = 0;
  replica_hit = 0;
  miss_count = 0;
  forward_count = 0;

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

  inet_pton(AF_INET,"172.18.216.35",&misip);
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

    main_timeregister(TIMEMODE_RUN_LATE,MDS_DECAY_TIME,0,mds_visit_decay);
    //main_timeregister(TIMEMODE_RUN_LATE,20,0,mds_check_replica);
    main_timeregister(TIMEMODE_RUN_LATE,60,0,mds_log_replica);
	//mdmd_stat_add_entry(STAT_DIR_MISS,STAT_DIR_MISS_STR,0);

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
    fprintf(stderr,"wrote %d to (ip:%u.%u.%u.%u)\n",i,(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);

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

    case MITOMD_CREATE_REPLICA:
      mds_create_replica(eptr, p);
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

    case CLTOMD_MKDIR:
      mds_mkdir(eptr, p);
      break;
    case MITOMD_MKDIR:
      mds_cl_mkdir(eptr, p);
      break;

    case CLTOMD_RMDIR:
      mds_rmdir(eptr,p);
      break;
    case MITOMD_RMDIR:
      mds_cl_rmdir(eptr,p);
      break;
    case CLTOMD_UNLINK:
      mds_unlink(eptr,p);
      break;
    case MITOMD_UNLINK:
      mds_cl_unlink(eptr,p);
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

    case CLTOMD_UTIMENS:
      mds_utimens(eptr,p);
      break;
    case MITOMD_UTIMENS:
      mds_cl_utimens(eptr,p);
      break;
    case CLTOMD_WRITE:
      mds_cl_write(eptr,p);
      break;

    case CLTOMD_LOGIN:
      mds_login(eptr,p);
      break;
    case MITOMD_LOGIN:
      mds_cl_login(eptr,p);
      break;
    case CLTOMD_ADD_USER:
      mds_add_user(eptr,p);
      break;
    case MITOMD_ADD_USER:
      mds_cl_add_user(eptr,p);
      break;
    case CLTOMD_DEL_USER:
      mds_del_user(eptr,p);
      break;
    case MITOMD_DEL_USER:
      mds_cl_del_user(eptr,p);
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

void mds_direct_pass_cl(mdsserventry* eptr,ppacket* p,int cmd){
  mdsserventry* ceptr = mds_entry_from_id(p->id);
  fprintf(stderr, "mds_direct_pass_cl entry id is %X\n", p->id);

  if(ceptr){
    ppacket* outp = createpacket_s(p->size,cmd,p->id);
    memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

    outp->next = ceptr->outpacket;
    ceptr->outpacket = outp;
  }
}

void mds_direct_pass_mi(ppacket* p,int cmd){
  const uint8_t* ptr = p->startptr;
  fprintf(stderr, "mds_direct_pass_mi entry id is %X\n", p->id);

  ppacket* outp = createpacket_s(p->size,cmd,p->id);
  memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

  outp->next = mdtomi->outpacket;
  mdtomi->outpacket = outp;
}

static void mis_update_attr(ppfile* f){
  int plen = strlen(f->path);
  ppacket* outp = createpacket_s(4+plen+sizeof(attr),MDTOMI_UPDATE_ATTR,-1);
  uint8_t* ptr2 = outp->startptr + HEADER_LEN;

  put32bit(&ptr2,plen);

  memcpy(ptr2,f->path,plen);
  ptr2 += plen;

  memcpy(ptr2,&f->a,sizeof(attr));

  outp->next = mdtomi->outpacket;
  mdtomi->outpacket = outp;
}

void mds_getattr(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_getattr\n");
  int plen;
  const uint8_t* ptr = p->startptr;
  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  path[plen] = 0;
  //syslog(LOG_WARNING, "mds_getattr: %s", path);

  fprintf(stderr,"path:%s\n",path);

  if(strcmp(path,"/")==0) {
      mds_direct_pass_mi(p,MDTOMI_GETATTR);
  } else {
    ppfile* f = lookup_file(path); //
    if(f == NULL){
      fprintf(stderr,"not found path:%s\n",path);
      //scan replica table 
      pprep* r = lookup_rep(path);
      if(r) { //get replica in rep table
        fprintf(stderr,"found path:%s in replica table\n", path);
        replica_hit++;
        r->visit_time += 1; //update visit info
        ppacket* outp = createpacket_s(4+sizeof(attr),MDTOCL_GETATTR,p->id);
        uint8_t *ptr2 = outp->startptr + HEADER_LEN;

        put32bit(&ptr2,0); //status
        memcpy(ptr2,&r->a,sizeof(attr));

        outp->next = eptr->outpacket;
        eptr->outpacket = outp;
      } else { //ask MIS
        mds_direct_pass_mi(p,MDTOMI_GETATTR);
      }
    } else { //return directly
      //syslog(LOG_WARNING,"mds_getattr: find path:%s",path);
      fprintf(stderr,"found path:%s\n",path);
      local_hit++;

      ppacket* outp = createpacket_s(4+sizeof(attr),MDTOCL_GETATTR,p->id);
      uint8_t *ptr2 = outp->startptr + HEADER_LEN;

      put32bit(&ptr2,0); //status
      memcpy(ptr2,&f->a,sizeof(attr));

      outp->next = eptr->outpacket;
      eptr->outpacket = outp;
    }
  }

  free(path);
}

void mds_cl_getattr(mdsserventry* eptr,ppacket* p){ //p->id? | Msg returned from MIS
  fprintf(stderr,"+mds_cl_getattr\n");
  const uint8_t* ptr = p->startptr;
  int flag = get32bit(&ptr);
  if(flag <= 0){ //directory or error, return directly
      mds_direct_pass_cl(eptr,p,MDTOCL_GETATTR);
      if(flag<0)
        miss_count++;
      return;
  } else { //regular file, flag is plen
      forward_count++;
      fprintf(stderr,"p->size=%d\n",p->size);
      int plen = flag;
      fprintf(stderr,"plen=%d\n",plen);
      char* path = malloc(plen+1); //should be freed
      memcpy(path,ptr,plen);
      path[plen] = 0;
      ptr += plen;
      uint32_t ip = get32bit(&ptr); //primary ip 

      //connect to Primary MDS of metadata.
      fprintf(stderr,"adding path:(%X,%s) to mdmd",ip,path);
      mdmd_add_entry(ip,path,MDMD_PATH_CACHE); //connect to Primary, TODO
      mdmdserventry* meptr = mdmdserventry_from_ip(ip); //get Primary entry
      if(meptr) {
          fprintf(stderr,"+retrieve eptr with Primary");
          mdmd_getattr(meptr,path,p->id);
      } else {
          fprintf(stderr,"+no eptr between Primary, return ENOENT\n");
          ppacket* outp = createpacket_s(4,MDTOCL_GETATTR,p->id); //p->id = Client id
          uint8_t *ptr2 = outp->startptr + HEADER_LEN;
          put32bit(&ptr2,-ENOENT);
          mds_direct_pass_cl(eptr, outp, MDTOCL_GETATTR); //potential bug TODO
      }
      free(path);
      return;
  }
  //mds_direct_pass_cl(eptr,p,MDTOCL_GETATTR);
}

void mds_create_replica(mdsserventry* eptr,ppacket* p){ //create replica of file entry
  fprintf(stderr,"+mds_create_replica\n");
  int plen;
  const uint8_t* ptr = p->startptr;
  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+1);
  memcpy(path,ptr,plen);
  path[plen] = 0;
  ptr+=plen;
  int repip = get32bit(&ptr);
  fprintf(stderr,"mds_create_replica path:%s replica ip:%X\n",path, repip);
  //use mdmd* function
  //primary connects to replica
  ppfile* f = lookup_file(path); //suppose the file exist in Primary mds
  attr a;
  if(f) {
    fprintf(stderr,"mds_create_replica path:%s found\n",path);
    a = f->a;
    rep* replica = (rep*)malloc(sizeof(rep));
    replica->rep_ip = repip;
    replica->visit_time = 0;
    replica->history = 0;
    replica->is_rep = 1;
    replica->next = f->rep_list;
    f->rep_list = replica;
  } else {
      fprintf(stderr, "mds_create_replica path %s not found\n", path);
      return;
  }
  //mdmd_connect( repip );
  mdmd_add_entry( repip, path, MDMD_PATH_CACHE ); //add entry in Primary mds, including path and rep_ip
  mdmd_send_attr(repip, f);
  
  //send file info to replica mds, including path, srcip and file attr
   //     if((meptr=mdmd_find_link(path)) != NULL){//use inter-mds connections, this process may be passed later.
   //       fprintf(stderr,"+using conns between mds and mds\n");
   //       mdmd_getattr(meptr,path,p->id);
   //     } else {

  free(path);
  return;
}


void mds_readdir(mdsserventry* eptr,ppacket* p){ //all fwd to MIS
  fprintf(stderr,"+mds_readdir\n");
  mds_direct_pass_mi(p,MDTOMI_READDIR);
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
  if(f){
    int mt = get32bit(&ptr);
    f->a.mode = mt;

    fprintf(stderr,"perm=%d%d%d\n",mt/0100 & 7,
                                   mt/0010 & 7,
                                   mt & 7);

    ppacket* outp = createpacket_s(4,MDTOCL_CHMOD,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;

    put32bit(&ptr2,0);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;

    if(eptr != mdtomi)
      mis_update_attr(f);
    rep* riter = f->rep_list;
    while(riter) {
        mdmd_update_attr(riter->rep_ip, f);
        riter = riter->next;
    }
  } else {
    if(eptr != mdtomi){ //from Client directly
      mds_direct_pass_mi(p,MDTOMI_CHMOD);
    } else { //Asked MIS before, MIS pretends as Client, return ENOENT
      ppacket* outp = createpacket_s(4,MDTOCL_CHMOD,p->id);
      uint8_t* ptr2 = outp->startptr+HEADER_LEN;

      put32bit(&ptr2,-ENOENT);

      outp->next = eptr->outpacket;
      eptr->outpacket = outp;
    }
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
  if(f){
    int uid = get32bit(&ptr);
    int gid = get32bit(&ptr);
    f->a.uid = uid;
    f->a.gid = gid;

    fprintf(stderr,"uid=%d,gid=%d\n",uid,gid);

    ppacket* outp = createpacket_s(4,MDTOCL_CHOWN,p->id);
    uint8_t* ptr2 = outp->startptr+HEADER_LEN;

    put32bit(&ptr2,0);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;

    if(eptr != mdtomi)
      mis_update_attr(f);
  } else {
    if(eptr != mdtomi){
      mds_direct_pass_mi(p,MDTOMI_CHOWN);
    } else {
      ppacket* outp = createpacket_s(4,MDTOCL_CHOWN,p->id);
      uint8_t* ptr2 = outp->startptr+HEADER_LEN;

      put32bit(&ptr2,-ENOENT);

      outp->next = eptr->outpacket;
      eptr->outpacket = outp;
    }
  }

  free(path);
}

void mds_cl_chown(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_CHOWN);
}

void mds_mkdir(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_mkdir\n");
  mds_direct_pass_mi(p,MDTOMI_MKDIR);
}

void mds_cl_mkdir(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_cl(eptr,inp,MDTOCL_MKDIR);
}

void mds_rmdir(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_rmdir\n");
  mds_direct_pass_mi(p,MDTOMI_RMDIR);
}

void mds_cl_rmdir(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_cl(eptr,inp,MDTOCL_RMDIR);
}

void mds_unlink(mdsserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mds_unlink\n");

  ppacket* p;
  char* path;
  int len;
  const uint8_t* inptr;
  int i;

  inptr = inp->startptr;
  len = get32bit(&inptr);
  printf("plen=%d\n",len);

  path = (char*)malloc((len+1)*sizeof(char));
  memcpy(path,inptr,len*sizeof(char));
  path[len] = 0;
  inptr += len;

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  //unlink locally
  if(f){
    for(i=0;i<f->chunks;i++){ //remove all chunks
      mdscs_delete_chunk(f->clist[i]);
    }
    remove_file(f);
    free_file(f);
  }
  //TODO
  //Delete all replicas in other replica MDSs, if exists
  //Scan the replica list in ppfile, rep_list

  //notify MIS
  if(mdtomi != eptr) //msg is from CLIENT, not pretended by MIS
    mds_direct_pass_mi(inp,MDTOMI_UNLINK);

  free(path);
}

void mds_cl_unlink(mdsserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mds_cl_unlink\n");

  mds_direct_pass_cl(eptr,inp,MDTOCL_UNLINK);
}

void mds_create(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_create\n");
  int plen;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  path[plen] = 0;
  ptr += plen;
  int mt = get32bit(&ptr);

  fprintf(stderr,"path:%s\n",path);
  fprintf(stderr,"mode:%o\n",mt);
  ppfile* f = lookup_file(path);
  if(f){
    ppacket* outp2 = createpacket_s(4, MDTOCL_CREATE, p->id);
    uint8_t *ptr3 = outp2->startptr + HEADER_LEN;
    put32bit(&ptr3, -EEXIST);

    fprintf(stderr,"file path:%s exist; status :%d\n",path, -EEXIST);
    outp2->next = eptr->outpacket;
    eptr->outpacket = outp2;
    free(path);

    return;
  } else {
    free(path);
    mds_direct_pass_mi(p,MDTOMI_CREATE); //check if exist in MIS
    return;
  }

  mds_direct_pass_mi(p,MDTOMI_CREATE);
}

void mds_cl_create(mdsserventry* eptr,ppacket* inp){ //yjy
  fprintf(stderr,"+mds_cl_create\n");
  //handle the message
  const uint8_t* ptr = inp->startptr;
  uint32_t status = get32bit(&ptr);
  if(status!=0) { //error num
    fprintf(stderr,"+status num %d\n", status);
    mds_direct_pass_cl(eptr,inp,MDTOCL_CREATE);
  } else {
      int plen;
      plen = get32bit(&ptr);
      char* path = (char*)malloc(plen+1);
      memcpy(path,ptr,plen);
      path[plen] = 0;
      ptr += plen;
      int mt = get32bit(&ptr);

      attr a;
      a.uid = a.gid = 0;
      a.atime = a.ctime = a.mtime = time(NULL);
      a.link = 1;
      a.size = 0;

      a.mode = mt; //use mode from client
      fprintf(stderr, "mds_mode : %o", mt);

      ppfile* nf = new_file(path,a);
      uint32_t ip2;
      tcpgetmyaddr(eptr->sock, &ip2);
      nf->srcip = ip2; //how to retrieve local ip, workaround: MIS send eptr->ip back XDDDD
      //fprintf(stderr,"primary (ip:%u.%u.%u.%u) \n",(*ip2>>24)&0xFF,(*ip2>>16)&0xFF,(*ip2>>8)&0xFF,*ip2&0xFF);
      //fprintf(stderr, "local ip is %X\n", *ip2); 
      nf->rep_list = NULL;
      nf->rep_cnt = 0;
      add_file(nf); //add to hash list
      fprintf(stderr, "add new file %s to hash\n", path);
      free(path);
      mds_direct_pass_cl(eptr,inp,MDTOCL_CREATE);
  }
}

void mds_open(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_mi(inp,MDTOMI_OPEN);
}

void mds_cl_open(mdsserventry* eptr,ppacket* inp){
  mds_direct_pass_cl(eptr,inp,MDTOCL_OPEN);
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

      mis_update_attr(f);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
  free(path);
}

void mds_cl_read_chunk_info(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_read_chunk_info,size=%d\n",p->size);

  int plen,mdsid,i;
  const uint8_t* ptr = p->startptr;
  ppacket* outp = NULL;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;

  if(mdtomi == eptr){
    mdsid = get32bit(&ptr);
    fprintf(stderr,"mdsid=%X\n",mdsid);
  }

  path[plen] = 0;
  fprintf(stderr,"plen=%d,path=%s\n",plen,path);
  ppfile* f = lookup_file(path);
  if(f == NULL){
    if(eptr != mdtomi){//file in another mds!
      mdmdserventry* meptr;
      if((meptr=mdmd_find_link(path)) != NULL){//use inter-mds connections
        fprintf(stderr,"+using conns between mds and mds\n");
        mdmd_read_chunk_info(meptr,path,p->id);
      } else {
        mds_direct_pass_mi(p,MDTOMI_READ_CHUNK_INFO);
      }
    } else { //no such file
      outp = createpacket_s(4+4,MDTOCL_READ_CHUNK_INFO,p->id);
      uint8_t* ptr2 = outp->startptr + HEADER_LEN;
      put32bit(&ptr2,-ENOENT);
      put32bit(&ptr2,mdsid);
    }
  } else {
    int totsize = 4+4+4+8*(f->chunks) + 4 + plen;
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
    fprintf(stderr,"plen=%d,path=%s\n",plen,path);

    put32bit(&ptr,plen);
    memcpy(ptr,path,plen);
    ptr += plen;

    if(eptr == mdtomi){
      put32bit(&ptr,mdsid);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
  free(path);
}

void mds_fw_read_chunk_info(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_fw_read_chunk_info\n");

  const uint8_t* ptr = p->startptr;
  int status = get32bit(&ptr);
  if(status == 0){
    uint32_t ip = get32bit(&ptr);
    int chunks = get32bit(&ptr);
    ptr += chunks * 8;
    if(p->size - 4 > 4+4+chunks*8){
      int plen = get32bit(&ptr);
      char* path = malloc(plen+10);
      memcpy(path,ptr,plen);
      path[plen] = 0;

      fprintf(stderr,"adding path:(%X,%s) to mdmd\n",ip,path);
      mdmd_add_entry(ip,path,MDMD_PATH_CACHE);

      char* dir = parentdir(path);
      fprintf(stderr,"adding dir:(%X,%s) to mdmd\n",ip,dir);
      mdmd_add_entry(ip,dir,MDMD_DIR_HEURISTIC);

      free(path);
      free(dir);
    }
  }

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
      mis_update_attr(f);
    }
  }

  if(outp){
    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  }
  free(path);
}

void mds_utimens(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_utimens\n");

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
  if(f){
    f->a.atime = get32bit(&ptr);
    f->a.mtime = get32bit(&ptr);

    if(eptr != mdtomi){
      ppacket* outp = createpacket_s(4,MDTOCL_UTIMENS,p->id);
      uint8_t* ptr2 = outp->startptr+HEADER_LEN;

      put32bit(&ptr2,0); //status

      outp->next = eptr->outpacket;
      eptr->outpacket = outp;

      fprintf(stderr,"updating mis info\n");
      mis_update_attr(f);
    }
  } else {
    mds_direct_pass_mi(p,MDTOMI_UTIMENS);
  }

  free(path);
}

void mds_cl_utimens(mdsserventry* eptr,ppacket* p){
  mds_direct_pass_cl(eptr,p,MDTOCL_UTIMENS);
}

void mds_cl_write(mdsserventry* eptr,ppacket* p){
  fprintf(stderr,"+mds_cl_write\n");

  int plen,off,st;
  const uint8_t* ptr = p->startptr;

  plen = get32bit(&ptr);
  printf("plen=%d\n",plen);

  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  ptr += plen;
  path[plen] = 0;

  fprintf(stderr,"path=%s\n",path);


  off = get32bit(&ptr);
  st = get32bit(&ptr);

  ppfile* f = lookup_file(path);
  if(f){
    int i = (f->a.size) / CHUNKSIZE;
    while(i < (off+st)/CHUNKSIZE && i < f->chunks){
      uint64_t previd = f->clist[i];
      mdscsserventry* ceptr = mdscs_find_serventry(previd);

      if(ceptr){
        ppacket* p = createpacket_s(8,MDTOCS_FILL_CHUNK,-1);
        uint8_t* ptr = p->startptr + HEADER_LEN;
        put64bit(&ptr,previd);

        p->next = ceptr->outpacket;
        ceptr->outpacket = p;
      }

      i++;
    }

    int oldsize = f->a.size;
    f->a.size = max(f->a.size,off+st);

    if(oldsize != f->a.size)
      mis_update_attr(f);
  }
  free(path);
}

void mds_login(mdsserventry* eptr,ppacket* p){
  //@TODO
}

void mds_cl_login(mdsserventry* eptr,ppacket* p){
  //@TODO
}

void mds_add_user(mdsserventry* eptr,ppacket* p){
  //@TODO
}

void mds_cl_add_user(mdsserventry* eptr,ppacket* p){
  //@TODO
}

void mds_del_user(mdsserventry* eptr,ppacket* p){
  //@TODO
}

void mds_cl_del_user(mdsserventry* eptr,ppacket* p){
  //@TODO
}

////Replica-related
void mds_visit_decay(void) {
    fprintf(stderr, "+mds_visit_decay\n");
    int i;
    for(i=0; i<HASHSIZE; ++i) {
        hashnode* n = tab[i];
        while(n) {
            ppfile* f = (ppfile*)(n->data);
            rep* r = f->rep_list;
            while(r) {
                if(r->is_rep) {
                    r=r->next;
                    continue;
                }
                //fprintf(stderr, "before update %s: ip:%X, history:%d, visit_time:%d\n", f->path, r->rep_ip, r->history, r->visit_time);
                r->history = r->history/2 + r->visit_time;
                r->visit_time = 0;
                //fprintf(stderr, "after update %s: ip:%X, history:%d, visit_time:%d\n", f->path, r->rep_ip, r->history, r->visit_time);
                r = r->next;
            }
            n = n->next;
        }
    }
    return;
}
    
void mds_check_replica(void) { //execute every slot, for replica deletion
    int i;
    fprintf(stderr, "+mds_check_replica\n");
    for(i=0; i<HASHSIZE; ++i) {
        hashnode* n = reptab[i];
        while(n) {
            pprep* f = (pprep*)(n->data);
            if(f->history+f->visit_time < 3 && f->age>=2) { //cold data, delete replica
                //inform Primary MDS and MIS
                fprintf(stderr, "delete cold replica %s\n", f->path);
                uint32_t ip = f->primaryip;
                //mdmd_add_entry(ip,path,MDMD_PATH_CACHE); //connect to Primary 
                mdmdserventry* meptr = mdmdserventry_from_ip(ip); //get Primary entry
                if(meptr) {
                    fprintf(stderr,"+inform Primary MDS %X\n", ip);
                    mdmd_send_delete(meptr,f->path);
                }

                //Replica MDS inform MIS the deletion. Alternative: MIS informed by Primary MDS
                fprintf(stderr,"+inform MIS \n");
                int plen = strlen(f->path);
                ppacket* outp = createpacket_s(4+plen,MDTOMI_DELETE_REPLICA,-1); //plen, path, ip
                delete_count++;
                uint8_t *ptr2 = outp->startptr + HEADER_LEN;
                put32bit(&ptr2,plen); //status
                memcpy(ptr2, f->path, plen);
                ptr2 += plen;
                outp->next = mdtomi->outpacket;
                mdtomi->outpacket = outp;               

                //delete locally
                fprintf(stderr,"delete locally \n");
                remove_rep(f); //remove from rep hash table
                n=n->next;
                continue;
            }
            f->history = f->history/2 + f->visit_time;
            f->age += 1;
            f->visit_time = 0;
            n = n->next;
        }
    }
    syslog(LOG_WARNING, "delete %d replica in this slot\n", delete_count);
    //fprintf(stderr, "delete %d replica in this slot\n", delete_count);
    total_delete += delete_count;
    //fprintf(stderr, "Totally delete %d replica\n", total_delete);
    delete_count = 0;
    return;
}
void mds_log_replica(void){
  syslog(LOG_WARNING, "Creating %d replicas in this period\n", create_count);
  //fprintf(stderr, "Creating %d replicas in this period\n", create_count);
  total_create += create_count;
  fprintf(stderr, "Totally created %d replicas\n", total_create);
  fprintf(stderr, "Totally delete %d replica\n", total_delete);
  fprintf(stderr, "Totally local_hit %d\n", local_hit);
  fprintf(stderr, "Totally replica_hit %d\n", replica_hit);
  fprintf(stderr, "Totally forward_count %d\n", forward_count);
  fprintf(stderr, "Totally miss_count %d\n", miss_count);
  create_count = 0;
}
