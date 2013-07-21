#ifndef __MDMD_STAT_H__
#define __MDMD_STAT_H__

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
#include <stdlib.h>

int mdmd_stat_init(void);
void mdmd_stat_term(void);

void mdmd_stat_add_entry(int key,char* desc,int c0);
void mdmd_stat_count(int key);
void mdmd_stat_countm(int key,int c);

void mdmd_stat_dump();

#endif
