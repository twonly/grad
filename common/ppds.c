#include "ppds.h"
#include <stdlib.h>

#if defined(__linux__)

char* strdup(const char*s){
  if(!s) return NULL;

  int n = strlen(s);
  char* ret = (char*)malloc(n + 10);
  strcpy(ret,s);

  return ret;
}

#endif
