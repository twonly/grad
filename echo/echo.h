#ifndef __ECHO_H__
#define __ECHO_H__

#include <syslog.h>
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include "slogger.h"
#include "massert.h"

enum {KILL,DATA};

#define MAXBUFSIZE 256

typedef struct _echoserventry{
  int sock; //0 - not active, 1 - read header, 2 - read packet
  uint8_t mode;
	uint32_t peerip;
  char buffer[MAXBUFSIZE];
  int writelen;
  int pdescpos;
  struct _echoserventry* next;
} echoserventry;

int echo_init(void);
void echo_write(echoserventry *eptr);
void echo_read(echoserventry *eptr);
void echo_term(void);
void echo_desc(struct pollfd *pdesc,uint32_t *ndesc);
void echo_serve(struct pollfd *pdesc);
#endif
