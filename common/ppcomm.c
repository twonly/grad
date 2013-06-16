#include "ppcomm.h"
#include "datapack.h"
#include <stdlib.h>

ppacket* createpacket_s(int size,int cmd){
  ppacket* p = (ppacket*)malloc(sizeof(ppacket)+8+size);
  p->size = size;
  p->cmd = cmd;
  p->buf = ((char*)p) + sizeof(ppacket);
  p->startptr = p->buf;
  p->bytesleft = size + 8;
  p->next = NULL;

  uint8_t* ptr = p->buf;
  put32bit(&ptr,size);
  put32bit(&ptr,cmd);

  return p;
}

ppacket* createpacket_r(int size,int cmd){
  ppacket* p = (ppacket*)malloc(sizeof(ppacket)+size);
  p->size = size;
  p->cmd = cmd;
  p->buf = ((char*)p) + sizeof(ppacket);
  p->startptr = p->buf;
  p->bytesleft = size;
  p->next = NULL;

  return p;
}
