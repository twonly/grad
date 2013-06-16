#include "ppcomm.h"
#include "datapack.h"
#include <stdlib.h>

ppacket* createpacket_s(int size,int cmd,int id){
  ppacket* p = (ppacket*)malloc(sizeof(ppacket)+HEADER_LEN+size);
  p->size = size;
  p->cmd = cmd;
  p->id = id;
  p->buf = ((char*)p) + sizeof(ppacket);
  p->startptr = p->buf;
  p->bytesleft = size + HEADER_LEN;
  p->next = NULL;

  uint8_t* ptr = p->buf;
  put32bit(&ptr,size);
  put32bit(&ptr,cmd);
  put32bit(&ptr,id);

  return p;
}

ppacket* createpacket_r(int size,int cmd,int id){
  ppacket* p = (ppacket*)malloc(sizeof(ppacket)+size);
  p->size = size;
  p->cmd = cmd;
  p->id = id;
  p->buf = ((char*)p) + sizeof(ppacket);
  p->startptr = p->buf;
  p->bytesleft = size;
  p->next = NULL;

  return p;
}
