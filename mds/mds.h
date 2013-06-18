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


#define MAXBUFSIZE 512

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
ppacket* mds_createpacket(int size,int cmd);

mdsserventry* mds_entry_from_id(int id);

void mds_getattr(mdsserventry* eptr,ppacket* p);
void mds_cl_getattr(mdsserventry* eptr,ppacket* p);

void mds_readdir(mdsserventry* eptr,ppacket* p);
void mds_cl_readdir(mdsserventry* eptr,ppacket* p);

void mds_access(mdsserventry* eptr,ppacket* p);
void mds_opendir(mdsserventry* eptr,ppacket* p);
void mds_mkdir(mdsserventry* eptr,ppacket* p);
void mds_releasedir(mdsserventry* eptr,ppacket* p);
void mds_rename(mdsserventry* eptr,ppacket* p);

void mds_chmod(mdsserventry* eptr,ppacket* p);
void mds_cl_chmod(mdsserventry* eptr,ppacket* p);
void mds_chown(mdsserventry* eptr,ppacket* p);
void mds_cl_chown(mdsserventry* eptr,ppacket* p);
void mds_chgrp(mdsserventry* eptr,ppacket* p);
void mds_cl_chgrp(mdsserventry* eptr,ppacket* p);

void mds_create(mdsserventry* eptr,ppacket* inp);
void mds_cl_create(mdsserventry* eptr,ppacket* inp);

void mds_open(mdsserventry* eptr,ppacket* inp);
void mds_cl_open(mdsserventry* eptr,ppacket* inp);

void mds_unlink(mdsserventry* eptr,ppacket* p);
void mds_noop(mdsserventry* eptr,ppacket* p);

void mds_cl_read_chunk_info(mdsserventry* eptr,ppacket* p);
void mds_cl_lookup_chunk(mdsserventry* eptr,ppacket* p);
void mds_cl_append_chunk(mdsserventry* eptr,ppacket* p);

void mds_fw_read_chunk_info(mdsserventry* eptr,ppacket* p);

#endif
