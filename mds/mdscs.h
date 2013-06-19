#ifndef __MDSCS_H__
#define __MDSCS_H__

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
#include "ppds.h"
#include "chunks.h"
#include "mdscs.h"

#define MAXBUFSIZE 512

typedef struct _mdscsserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  linklist* clist;
  int space,availspace,chunks;

  struct _mdscsserventry* next;
} mdscsserventry;

mdscsserventry* mdscs_find_serventry(uint64_t chunkid);// is this needed?
void mdscs_new_chunk(mdschunk** c);
void mdscs_delete_chunk(uint64_t chunkid);
int mdscs_append_chunk(ppfile* f,mdschunk* id);
int mdscs_pop_chunk(ppfile* f,uint64_t* id);

int mdscs_init(void);
void mdscs_term(void);
void mdscs_desc(struct pollfd *pdesc,uint32_t *ndesc);
void mdscs_serve(struct pollfd *pdesc);

void mdscs_write(mdscsserventry *eptr);
void mdscs_read(mdscsserventry *eptr);

void mdscs_gotpacket(mdscsserventry* eptr,ppacket* p);

void mdscs_register(mdscsserventry* eptr,ppacket* p);
void mdscs_update_status(mdscsserventry* eptr,ppacket* p);

#endif
