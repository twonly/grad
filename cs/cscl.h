#ifndef __CSCL_H__
#define __CSCL_H__

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

enum {KILL,HEADER,DATA};

#define MAXBUFSIZE 512

typedef struct _csclserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  struct _csclserventry* next;
} csclserventry;

int cscl_init(void);
void cscl_term(void);
void cscl_desc(struct pollfd *pdesc,uint32_t *ndesc);
void cscl_serve(struct pollfd *pdesc);

void cscl_write(csclserventry *eptr);
void cscl_read(csclserventry *eptr);

void cscl_gotpacket(csclserventry* eptr,ppacket* p);

#endif
