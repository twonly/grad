#ifndef __MAIN_H__
#define __MAIN_H__

#include <poll.h>
#include <inttypes.h>

#define TIMEMODE_SKIP_LATE 0
#define TIMEMODE_RUN_LATE 1

void main_pollregister (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *));

void main_destructregister (void (*fun)(void));

void* main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void));

uint64_t main_utime();

uint32_t main_time();

#endif
