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

enum {KILL,HEADER,DATA};

#define MAXBUFSIZE 512

typedef struct _mdscsserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  chunk* clist;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  struct _mdscsserventry* next;
} mdscsserventry;

int mdscs_init(void);
void mdscs_term(void);
void mdscs_desc(struct pollfd *pdesc,uint32_t *ndesc);
void mdscs_serve(struct pollfd *pdesc);

void mdscs_write(mdscsserventry *eptr);
void mdscs_read(mdscsserventry *eptr);

void mdscs_gotpacket(mdscsserventry* eptr,ppacket* p);

#endif
