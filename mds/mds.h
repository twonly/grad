#ifndef __MDS_H__
#define __MDS_H__

#include <syslog.h>
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "slogger.h"
#include "massert.h"
#include "ppcomm.h"
#include "ppfile.h"
#include <signal.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "chunks.h"
#include "mdscs.h"
#include "mdmd.h"

#define MAXBUFSIZE 512

#define MDS_DECAY_TIME 60 //second
#define MDS_REPLICA_SLOT 10 //second

int create_count;
int delete_count;
int total_create;
int total_delete;
int local_hit;
int replica_hit;
int miss_count;
int forward_count;

typedef struct _mdsserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  struct _mdsserventry* next;
} mdsserventry;

int mds_init(void);
void mds_term(void);
void mds_desc(struct pollfd *pdesc,uint32_t *ndesc);
void mds_serve(struct pollfd *pdesc);

void mds_write(mdsserventry *eptr);
void mds_read(mdsserventry *eptr);

void mds_gotpacket(mdsserventry* eptr,ppacket* p);

mdsserventry* mds_entry_from_id(int id);

void mds_getattr(mdsserventry* eptr,ppacket* p);
void mds_cl_getattr(mdsserventry* eptr,ppacket* p);

void mds_create_replica(mdsserventry* eptr,ppacket* p);
void mds_mi_create_replica(mdsserventry* eptr,ppacket* p);

void mds_readdir(mdsserventry* eptr,ppacket* p);
void mds_cl_readdir(mdsserventry* eptr,ppacket* p);

void mds_access(mdsserventry* eptr,ppacket* p);
void mds_opendir(mdsserventry* eptr,ppacket* p);
void mds_mkdir(mdsserventry* eptr,ppacket* p);
void mds_releasedir(mdsserventry* eptr,ppacket* p);

void mds_chmod(mdsserventry* eptr,ppacket* p);
void mds_cl_chmod(mdsserventry* eptr,ppacket* p);
void mds_chown(mdsserventry* eptr,ppacket* p);
void mds_cl_chown(mdsserventry* eptr,ppacket* p);
void mds_chgrp(mdsserventry* eptr,ppacket* p);
void mds_cl_chgrp(mdsserventry* eptr,ppacket* p);
void mds_utimens(mdsserventry* eptr,ppacket* p);
void mds_cl_utimens(mdsserventry* eptr,ppacket* p);

void mds_mkdir(mdsserventry* eptr,ppacket* inp);
void mds_cl_mkdir(mdsserventry* eptr,ppacket* inp);
void mds_create(mdsserventry* eptr,ppacket* inp);
void mds_cl_create(mdsserventry* eptr,ppacket* inp);
void mds_unlink(mdsserventry* eptr,ppacket* inp);
void mds_cl_unlink(mdsserventry* eptr,ppacket* inp);
void mds_rmdir(mdsserventry* eptr,ppacket* inp);
void mds_cl_rmdir(mdsserventry* eptr,ppacket* inp);

void mds_open(mdsserventry* eptr,ppacket* inp);
void mds_cl_open(mdsserventry* eptr,ppacket* inp);

void mds_cl_write(mdsserventry* eptr,ppacket* inp);

void mds_unlink(mdsserventry* eptr,ppacket* p);
void mds_noop(mdsserventry* eptr,ppacket* p);

void mds_cl_read_chunk_info(mdsserventry* eptr,ppacket* p);
void mds_cl_lookup_chunk(mdsserventry* eptr,ppacket* p);
void mds_cl_append_chunk(mdsserventry* eptr,ppacket* p);
void mds_cl_pop_chunk(mdsserventry* eptr,ppacket* p);

void mds_fw_read_chunk_info(mdsserventry* eptr,ppacket* p);

//@TODO
void mds_login(mdsserventry* eptr,ppacket* p);
void mds_cl_login(mdsserventry* eptr,ppacket* p);
void mds_add_user(mdsserventry* eptr,ppacket* p);
void mds_cl_add_user(mdsserventry* eptr,ppacket* p);
void mds_del_user(mdsserventry* eptr,ppacket* p);
void mds_cl_del_user(mdsserventry* eptr,ppacket* p);

//replica related
void mds_visit_decay(void);
void mds_check_replica(void);
void mds_log_replica(void);

void mds_direct_pass_mi(ppacket* p,int cmd);
#endif
