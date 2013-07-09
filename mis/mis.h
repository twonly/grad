#ifndef __MIS_H__
#define __MIS_H__

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

#include "main.h"

enum {KILL,HEADER,DATA};

#define MAXBUFSIZE 512

typedef struct _misserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  struct _misserventry* next;
} misserventry;

int mis_init(void);
void mis_term(void);
void mis_desc(struct pollfd *pdesc,uint32_t *ndesc);
void mis_serve(struct pollfd *pdesc);

void mis_write(misserventry *eptr);
void mis_read(misserventry *eptr);

void mis_gotpacket(misserventry* eptr,ppacket* p);
ppacket* mis_createpacket(int size,int cmd);

misserventry* mis_entry_from_ip(int ip);

void mis_getattr(misserventry* eptr,ppacket* p);
void mis_opendir(misserventry* eptr,ppacket* p);
void mis_readdir(misserventry* eptr,ppacket* p);
void mis_mkdir(misserventry* eptr,ppacket* p);
void mis_releasedir(misserventry* eptr,ppacket* p);

void mis_create(misserventry* eptr,ppacket* p);
void mis_open(misserventry* eptr,ppacket* p);

void mis_chmod(misserventry* eptr,ppacket* p);
void mis_chgrp(misserventry* eptr,ppacket* p);
void mis_chown(misserventry* eptr,ppacket* p);
void mis_utimens(misserventry* eptr,ppacket* p);

void mis_update_attr(misserventry* eptr,ppacket* p);

void mis_rmdir(misserventry* eptr,ppacket* p);
void mis_unlink(misserventry* eptr,ppacket* p);
void mis_noop(misserventry* eptr,ppacket* p);

void mis_fw_read_chunk_info(misserventry* eptr,ppacket* p);
void mis_rfw_read_chunk_info(misserventry* eptr,ppacket* p);
void mis_fw_lookup_chunk(misserventry* eptr,ppacket* p);
void mis_rfw_lookup_chunk(misserventry* eptr,ppacket* p);
void mis_fw_append_chunk(misserventry* eptr,ppacket* p);
void mis_rfw_append_chunk(misserventry* eptr,ppacket* p);

//@TODO
void mis_login(misserventry* eptr,ppacket* p);
void mis_add_user(misserventry* eptr,ppacket* p);
void mis_del_user(misserventry* eptr,ppacket* p);

#endif
