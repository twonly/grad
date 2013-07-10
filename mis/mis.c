#include "mis.h"
#include "datapack.h"
#include "mis_fs.h"

static misserventry* misservhead = NULL;

static int lsock;
static int lsockpdescpos;

static ppfile* root;

void mis_fs_demo_init(void){
  attr a;
  int i;

  a.uid = a.gid = 0;
  a.atime = a.ctime = a.mtime = time(NULL);
  a.link = 1;
  a.size = 1;

  a.mode = 0777 | S_IFDIR;
  root = new_file("/",a);
  add_file(root);

    //nf->srcip = eptr->peerip;

}

static void timeentry_test(void){
  fprintf(stderr,"TEST:%d\n",main_time());
}

int mis_init(void){
  lsock = tcpsocket();
  if (lsock<0) {
    mfs_errlog(LOG_ERR,"mis module: can't create socket");
    return -1;
  }
  tcpnonblock(lsock);
  tcpnodelay(lsock);
  tcpreuseaddr(lsock);

  lsockpdescpos = -1;

	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		mfs_errlog_silent(LOG_NOTICE,"mis module: can't set accept filter");
	}
	if (tcpstrlisten(lsock,"*",MIS_PORT_STR,100)<0) {
		mfs_errlog(LOG_ERR,"mis module: can't listen on socket");
		return -1;
	}
  
  fprintf(stderr,"listening on port %s\n",MIS_PORT_STR);

	main_destructregister(mis_term);
	main_pollregister(mis_desc,mis_serve);
  /*main_timeregister(TIMEMODE_RUN_LATE,10,0,timeentry_test);*/

  mis_fs_demo_init();

  return 0;
}

void mis_serve(struct pollfd *pdesc) {
	misserventry *eptr;

	if (lsockpdescpos >=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		int ns=tcpaccept(lsock);
		if (ns<0) {
			mfs_errlog_silent(LOG_NOTICE,"mis module: accept error");
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
      eptr->bytesleft = HEADER_LEN;
      eptr->startptr = eptr->headbuf;

      fprintf(stderr,"mds(ip:%u.%u.%u.%u) connected\n",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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

	fprintf(stderr,"mis module: closing %s:%s\n","*",MIS_PORT_STR);
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

      mis_gotpacket(eptr,eptr->inpacket);
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

void mis_write(misserventry *eptr) {
	int32_t i;

  while(eptr->outpacket){
		i=write(eptr->sock,eptr->outpacket->startptr,eptr->outpacket->bytesleft);

		if (i<0) {
			if (errno!=EAGAIN) {
				mfs_arg_errlog_silent(LOG_NOTICE,"mis module: (ip:%u.%u.%u.%u) write error",(eptr->peerip>>24)&0xFF,(eptr->peerip>>16)&0xFF,(eptr->peerip>>8)&0xFF,eptr->peerip&0xFF);
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
  fprintf(stderr,"dispatching packet ,size:%d,cmd:%X\n",p->size,p->cmd);
  switch(p->cmd){
    case MDTOMI_GETATTR:
      mis_getattr(eptr,p);
      break;
    case MDTOMI_READDIR:
      mis_readdir(eptr,p);
      break;
    case MDTOMI_MKDIR:
      mis_mkdir(eptr,p);
      break;
    case MDTOMI_RMDIR:
      mis_rmdir(eptr,p);
      break;
    case MDTOMI_UNLINK:
      mis_unlink(eptr,p);
      break;
    case MDTOMI_CREATE:
      mis_create(eptr,p);
      break;
    case MDTOMI_OPEN:
      mis_open(eptr,p);
      break;
    case MDTOMI_UPDATE_ATTR:
      mis_update_attr(eptr,p);
      break;
    case MDTOMI_CHMOD:
      mis_chmod(eptr,p);
      break;
    case MDTOMI_CHOWN:
      mis_chown(eptr,p);
      break;

    case MDTOMI_READ_CHUNK_INFO:
      mis_fw_read_chunk_info(eptr,p);
      break;
    case MDTOCL_READ_CHUNK_INFO:
      mis_rfw_read_chunk_info(eptr,p);
      break;

    case MDTOMI_UTIMENS:
      mis_utimens(eptr,p);
      break;
    case MDTOMI_ADD_USER:
      mis_add_user(eptr,p);
      break;
    case MDTOMI_DEL_USER:
      mis_del_user(eptr,p);
      break;
    case MDTOMI_LOGIN:
      mis_login(eptr,p);
      break;
  }

  fprintf(stderr,"\n\n");
}

void mis_getattr(misserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mis_getattr\n");

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

  ppfile* f = lookup_file(path);
  if(f == NULL){
    p = createpacket_s(4,MITOMD_GETATTR,inp->id);

    uint8_t* ptr = p->startptr + HEADER_LEN;
    fprintf(stderr, "can not find the path, status=%d\n",-ENOENT);
    put32bit(&ptr,-ENOENT);
  } else {
    fprintf(stderr, "found the path, status=%d\n", 0);
    p = createpacket_s(4+sizeof(attr),MITOMD_GETATTR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0);
    memcpy(ptr,&f->a,sizeof(attr));
  }

  free(path);
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

  ppfile* f = lookup_file(path);
  if(f == NULL){
    printf("mis readdir path=%s, not exist\n",path);
    p = createpacket_s(4,MITOMD_READDIR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    if(S_ISDIR(f->a.mode)){
      ppfile* cf;
      int totsize = 8;
      int nfiles = 0;

      for(cf = f->child;cf;cf = cf->next){
        totsize += 4 + strlen(cf->name);
        nfiles++;
      }
      fprintf(stderr, "mis readdir %d files\n",nfiles);

      p = createpacket_s(totsize,MITOMD_READDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,0); //status
      put32bit(&ptr,nfiles);

      for(cf = f->child;cf;cf = cf->next){
        int len = strlen(cf->name);

        put32bit(&ptr,len);
        memcpy(ptr,cf->name,len);
        ptr += len;
      }
    } else {
      p = createpacket_s(4,MITOMD_READDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOTDIR);
    }
  }

  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_mkdir(misserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mis_mkdir\n");

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
  inptr += len;
  mode_t mt = get32bit(&inptr);

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f){
    fprintf(stderr,"directory exists\n");

    p = createpacket_s(4,MITOMD_MKDIR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-EEXIST);

    goto end;
  } else { //find the parent directory
    char* dir;
    if(len > 1){
      dir = &path[len-1];
      while(dir >= path && *dir != '/') dir--;

      int dirlen = dir - path;
      if( dirlen==0 ) dirlen+=1; //for /dir
      dir = strdup(path);
      dir[dirlen] = 0;
    } else {
      dir = strdup("/");
    }


    printf("parent dir=%s\n",dir); //parent dir

    f = lookup_file(dir);
    if(!f){
      p = createpacket_s(4,MITOMD_MKDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOENT);
      
      free(dir);
      goto end;
    } else {
      if(!S_ISDIR(f->a.mode)){ //exist but not directory
        p = createpacket_s(4,MITOMD_MKDIR,inp->id);
        uint8_t* ptr = p->startptr + HEADER_LEN;
        put32bit(&ptr,-ENOTDIR);

        free(dir);
        goto end;
      }
    }

    attr a;

    a.uid = a.gid = 0;
    a.atime = a.ctime = a.mtime = time(NULL);
    a.link = 1;
    a.size = 0;

    a.mode = mt; //| S_IFREG; //use mode from client
    syslog(LOG_WARNING, "mis_mode : %o", mt);

    ppfile* nf = new_file(path,a);
    add_file(nf); //  like "/dir"
    nf->next = f->child;
    f->child = nf;

    nf->srcip = eptr->peerip; //not necessary for dir

    p = createpacket_s(4+strlen(path)+4+4,MITOMD_MKDIR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0);
    put32bit(&ptr,strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr += strlen(path);

    syslog(LOG_WARNING, "mis_mode sent to mds: %o", mt);
    put32bit(&ptr, mt);

    free(dir);
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_rmdir(misserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mis_rmdir\n");

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
  if(f){
    if(!S_ISDIR(f->a.mode)) {
      p = createpacket_s(4,MITOMD_RMDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOTDIR);
      goto end;
    }

    if( f->child != NULL ) {
        p = createpacket_s(4,MITOMD_RMDIR,inp->id);
        uint8_t* ptr = p->startptr + HEADER_LEN;
        put32bit(&ptr,-ENOTEMPTY);
        fprintf(stderr,"dir is not empty\n");
        goto end;
    }

    char* dir;
    if(len > 1){
      dir = &path[len-1];
      while(dir >= path && *dir != '/') dir--;

      int dirlen = dir - path;
      if(dirlen==0) dirlen+=1;
      dir = strdup(path);
      dir[dirlen] = 0;
    } else {
      dir = strdup("/");
    }

    printf("dir=%s\n",dir);
    ppfile* pf = lookup_file(dir);
    if(!pf){
      p = createpacket_s(4,MITOMD_RMDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOENT);
      free(dir);
      goto end;
    } else if(!S_ISDIR(pf->a.mode)){ //exist but not directory
      p = createpacket_s(4,MITOMD_RMDIR,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOTDIR);
      free(dir);
      goto end;
    }

    fprintf(stderr,"rmdir %s\n", path);

    p = createpacket_s(4+strlen(path)+4+4,MITOMD_RMDIR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;

    put32bit(&ptr, 0);
    put32bit(&ptr, strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr += strlen(path);

    remove_child(pf,f);
    free_file(f);
    free(dir);
  } else { //not exist
    fprintf(stderr,"file not exists\n");
    p = createpacket_s(4,MITOMD_RMDIR,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr, -ENOENT);
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_unlink(misserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mis_unlink\n");

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
  if(f){
    if(S_ISDIR(f->a.mode)) {
        fprintf(stderr,"path is DIR\n");
        p = createpacket_s(4,MITOMD_UNLINK,inp->id);
        uint8_t* ptr = p->startptr + HEADER_LEN;
        put32bit(&ptr,-EISDIR);
        goto end;
    }

    char* dir;
    if(len > 1){
      dir = &path[len-1];
      while(dir >= path && *dir != '/') dir--;

      int dirlen = dir - path;
      if(dirlen==0) dirlen+=1;
      dir = strdup(path);
      dir[dirlen] = 0;
    } else {
      dir = strdup("/");
    }

    printf("dir=%s\n",dir);
    ppfile* pf = lookup_file(dir); //must always exist as a directory

    if(!pf){ //parent dir not exist
      p = createpacket_s(4,MITOMD_UNLINK,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOENT);
      free(dir);
      goto end;
    }

    if(f->srcip != eptr->peerip){
      misserventry* ceptr = mis_entry_from_ip(f->srcip);

      if(ceptr){
        p = createpacket_s(4+4+len,CLTOMD_UNLINK,0); //no client will use ip address 0.0.0.0
        uint8_t* ptr = p->startptr + HEADER_LEN;

        put32bit(&ptr,0);
        put32bit(&ptr,len);
        memcpy(ptr,path,len);
        ptr += len;

        p->next = ceptr->outpacket;
        ceptr->outpacket = p;
      }
    }

    p = createpacket_s(4+len+4,MITOMD_UNLINK,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;

    put32bit(&ptr, 0);
    put32bit(&ptr, len);
    memcpy(ptr, path, len);
    ptr += len;

    remove_child(pf, f);
    free_file(f);
    free(dir);
  } else { //not exist
    fprintf(stderr,"file not exists\n");
    p = createpacket_s(4,MITOMD_UNLINK,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr, -ENOENT);
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_create(misserventry* eptr,ppacket* inp){
  fprintf(stderr,"+mis_create\n");

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
  inptr += len;
  int mt = get32bit(&inptr);

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f){
    fprintf(stderr,"file exists\n");

    p = createpacket_s(4,MITOMD_CREATE,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-EEXIST);

    goto end;
  } else { //find the parent directory
    char* dir;
    if(len > 1){
      dir = &path[len-1];
      while(dir >= path && *dir != '/') dir--;

      int dirlen = dir - path;
      if(dirlen==0) dirlen+=1;
      dir = strdup(path);
      dir[dirlen] = 0;
    } else {
      dir = strdup("/");
    }


    printf("dir=%s\n",dir);
    f = lookup_file(dir);
    if(!f){ //always can't find f
      p = createpacket_s(4,MITOMD_CREATE,inp->id);
      uint8_t* ptr = p->startptr + HEADER_LEN;
      put32bit(&ptr,-ENOENT);

      free(dir);
      goto end;
    } else {
      if(!S_ISDIR(f->a.mode)){ //exist but not directory
        p = createpacket_s(4,MITOMD_CREATE,inp->id);
        uint8_t* ptr = p->startptr + HEADER_LEN;
        put32bit(&ptr,-ENOTDIR);

        free(dir);
        goto end;
      }
    }

    attr a;

    a.uid = a.gid = 0;
    a.atime = a.ctime = a.mtime = time(NULL);
    a.link = 1;
    a.size = 0;

    a.mode = mt; //| S_IFREG; //use mode from client
    fprintf(stderr, "mis_mode : %o", mt);

    ppfile* nf = new_file(path,a);
    nf->srcip = eptr->peerip;
    fprintf(stderr, "nf->srcip is %X, eptr->peerip is %X\n", nf->srcip, eptr->peerip);
    add_file(nf); //add to hash list
    nf->next = f->child;
    f->child = nf;

    p = createpacket_s(4+strlen(path)+4+4,MITOMD_CREATE,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0);
    put32bit(&ptr,strlen(path));
    memcpy(ptr, path, strlen(path));
    ptr += strlen(path);
    put32bit(&ptr, mt);

    free(dir);
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

//@TODO need to add access control
void mis_open(misserventry* eptr,ppacket* inp){
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

  ppfile* f = lookup_file(path);
  if(!f){
    p = createpacket_s(4,MITOMD_OPEN,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    p = createpacket_s(4,MITOMD_OPEN,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0);
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_update_attr(misserventry* eptr,ppacket* inp){ //no need to send back
  fprintf(stderr,"+mis_update_attr\n");

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
  inptr += len;
  path[len] = 0;

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f){
    attr a;
    memcpy(&a,inptr,sizeof(attr));
    f->a = a;
  }

  free(path);
}

misserventry* mis_entry_from_ip(int ip){
  misserventry* eptr = misservhead;
  
  fprintf(stderr,"+misserventry:ip=%X\n",ip);

  while(eptr){
    fprintf(stderr,"peerip=%X\n",eptr->peerip);

    if(eptr->peerip == ip)
      return eptr;

    eptr = eptr->next;
  }

  return eptr;
}

//@TODO need to add access control
void mis_chmod(misserventry* eptr,ppacket* inp){
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
  inptr += len;
  path[len] = 0;

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(!f){
    p = createpacket_s(4,MITOMD_CHMOD,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    int mt = get32bit(&inptr);
    f->a.mode = mt;

    fprintf(stderr,"mt=%o\n",mt);

    p = createpacket_s(4,MITOMD_CHMOD,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0); //status

    fprintf(stderr,"f->srcip=%X,eptr->peerip=%X\n",f->srcip,eptr->peerip);

    if(f->srcip != eptr->peerip){//update mds info
      misserventry* ceptr = mis_entry_from_ip(f->srcip);
      fprintf(stderr,"updating mds:%X,ceptr is %X\n",f->srcip,ceptr);

      if(ceptr){
        ppacket* outp = createpacket_s(inp->size,CLTOMD_CHMOD,inp->id);
        memcpy(outp->startptr+HEADER_LEN,inp->startptr,inp->size);

        outp->next = ceptr->outpacket;
        ceptr->outpacket = outp;
      }
    }
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

//@TODO need to add access control
void mis_chown(misserventry* eptr,ppacket* inp){
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
  inptr += len;
  path[len] = 0;

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(!f){
    p = createpacket_s(4,MITOMD_CHMOD,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    int uid = get32bit(&inptr);
    int gid = get32bit(&inptr);
    f->a.uid = uid;
    f->a.gid = gid;

    p = createpacket_s(4,MITOMD_CHMOD,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0);

    fprintf(stderr,"f->srcip=%X,eptr->peerip=%X\n",f->srcip,eptr->peerip);

    if(f->srcip != eptr->peerip){//update mds info
      misserventry* ceptr = mis_entry_from_ip(f->srcip);
      fprintf(stderr,"updating mds:%X,ceptr is %X\n",f->srcip,ceptr);

      if(ceptr){
        ppacket* outp = createpacket_s(inp->size,CLTOMD_CHMOD,inp->id);
        memcpy(outp->startptr+HEADER_LEN,inp->startptr,inp->size);

        outp->next = ceptr->outpacket;
        ceptr->outpacket = outp;
      }
    }
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_fw_read_chunk_info(misserventry* eptr,ppacket* p){
  fprintf(stderr,"+mis_fw_read_chunk_info\n");

  int plen,mdsid,i;
  const uint8_t* ptr = p->startptr;
  ppacket* outp = NULL;

  plen = get32bit(&ptr);
  char* path = (char*)malloc(plen+10);
  memcpy(path,ptr,plen);
  path[plen] = 0;
  ptr += plen;

  fprintf(stderr,"path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(f == NULL){
    fprintf(stderr,"no such file\n");

    outp = createpacket_s(4,MITOMD_READ_CHUNK_INFO,p->id);
    uint8_t* ptr2 = outp->startptr + HEADER_LEN;
    put32bit(&ptr2,-ENOENT);

    outp->next = eptr->outpacket;
    eptr->outpacket = outp;
  } else {
    fprintf(stderr,"locating serventry\n");

    misserventry* ceptr = mis_entry_from_ip(f->srcip);

    if(ceptr){
      outp = createpacket_s(p->size+4,CLTOMD_READ_CHUNK_INFO,p->id);
      memcpy(outp->startptr + HEADER_LEN,p->startptr,p->size);
      uint8_t* ptr2 = outp->startptr + HEADER_LEN + p->size;
      put32bit(&ptr2,eptr->peerip);

      outp->next = ceptr->outpacket;
      ceptr->outpacket = outp;

      fprintf(stderr,"forwarding to %X from %X\n",f->srcip,eptr->peerip);
      fprintf(stderr,"size=%d\n",outp->size);
    } else {
      //@TODO: add error handling
    }
  }
}

void mis_rfw_read_chunk_info(misserventry* eptr,ppacket* p){
  fprintf(stderr,"+mis_rfw_read_chunk_info\n");

  const uint8_t* ptr = p->startptr + p->size - 4;
  uint32_t ip = get32bit(&ptr);

  misserventry* ceptr = mis_entry_from_ip(ip);
  fprintf(stderr,"+ip=%X\n",ip);

  if(!ceptr){
    //@TODO: add error handling
    fprintf(stderr,"+ip=%X,serventry not found\n",ip);
    return;
  }

  ppacket* outp = createpacket_s(p->size,MITOMD_READ_CHUNK_INFO,p->id);
  memcpy(outp->startptr + HEADER_LEN,p->startptr,p->size);
  ptr = p->startptr;
  int status = get32bit(&ptr);
  if(status == 0){
    uint8_t* ptr2 = outp->startptr + 4 + HEADER_LEN;
    fprintf(stderr,"forwarding peerip=%X\n",eptr->peerip);
    put32bit(&ptr2,eptr->peerip);//remote mds
  }

  outp->next = ceptr->outpacket;
  ceptr->outpacket = outp;
}

//@TODO need to add access control
void mis_utimens(misserventry* eptr,ppacket* inp){
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
  inptr += len;
  path[len] = 0;

  printf("path=%s\n",path);

  ppfile* f = lookup_file(path);
  if(!f){
    p = createpacket_s(4,MITOMD_UTIMENS,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,-ENOENT);
  } else {
    f->a.atime = get32bit(&inptr);
    f->a.mtime = get32bit(&inptr);

    p = createpacket_s(4,MITOMD_UTIMENS,inp->id);
    uint8_t* ptr = p->startptr + HEADER_LEN;
    put32bit(&ptr,0); //status

    if(f->srcip != eptr->peerip){//update mds info
      misserventry* ceptr = mis_entry_from_ip(f->srcip);

      if(ceptr){
        ppacket* outp = createpacket_s(inp->size,CLTOMD_UTIMENS,inp->id);
        memcpy(outp->startptr+HEADER_LEN,p->startptr,p->size);

        outp->next = ceptr->outpacket;
        ceptr->outpacket = outp;
      }
    }
  }

end:
  free(path);
  p->next = eptr->outpacket;
  eptr->outpacket = p;
}

void mis_login(misserventry* eptr,ppacket* p){
  //@TODO
}

void mis_add_user(misserventry* eptr,ppacket* p){
  //@TODO
}

void mis_del_user(misserventry* eptr,ppacket* p){
  //@TODO
}

