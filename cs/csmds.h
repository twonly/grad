#ifndef __CSMDS_H__
#define __CSMDS_H__

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
#include <signal.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "chunks.h"


typedef struct _csmdsserventry{
  int sock; 
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  struct _csmdsserventry* next;
} csmdsserventry;

int csmds_init(void);
void csmds_term(void);
void csmds_desc(struct pollfd *pdesc,uint32_t *ndesc);
void csmds_serve(struct pollfd *pdesc);

void csmds_gotpacket(csmdsserventry* eptr,ppacket* p);

void csmds_write(csmdsserventry *eptr);
void csmds_read(csmdsserventry *eptr);

void csmds_update_status(csmdsserventry* eptr,ppacket* p);

void csmds_register(csmdsserventry* eptr,ppacket* p);
void csmds_create(csmdsserventry* eptr,ppacket* p);
void csmds_delete(csmdsserventry* eptr,ppacket* p);

#endif
