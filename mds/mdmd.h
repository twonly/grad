#ifndef __mdmd_H__
#define __mdmd_H__

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
#include <pthread.h>
#include "pcqueue.h"
#include "mds.h"
#include "ppds.h"

#define MAX_MDS_CONN 50
#define CONN_TIMEOUT 500 //in ms

#define MDMD_HASHSIZE 100

typedef struct _mdmdserventry{
  int sock;
	uint32_t peerip;

  uint8_t mode; //0 - not active, 1 - read header, 2 - read packet
  int pdescpos;

  ppacket* inpacket;
  ppacket* outpacket;

  uint8_t headbuf[20];
  uint8_t* startptr;
  int bytesleft;

  int atime;
  int type; //1:incoming; 2:outgoing

  hashnode* htab[MDMD_HASHSIZE];

  struct _mdmdserventry* next;
} mdmdserventry;

int mdmd_init(void);
void mdmd_term(void);
void mdmd_desc(struct pollfd *pdesc,uint32_t *ndesc);
void mdmd_serve(struct pollfd *pdesc);

void* mdmd_conn_thread(void*);

void mdmd_write(mdmdserventry *eptr);
void mdmd_read(mdmdserventry *eptr);

void mdmd_add_path(uint32_t ip,char* path);
mdmdserventry* mdmd_find_link(char* path);

void mdmd_gotpacket(mdmdserventry* eptr,ppacket* p);

void mdmd_read_chunk_info(mdmdserventry* eptr,char* path,int id);
void mdmd_s2c_read_chunk_info(mdmdserventry* eptr,ppacket* p);
void mdmd_c2s_read_chunk_info(mdmdserventry* eptr,ppacket* p);

void mdmdserventry_add_path(mdmdserventry* eptr,char* path);
int mdmdserventry_has_path(mdmdserventry* eptr,char* path);
void mdmdserventry_free(mdmdserventry* eptr);

#endif
