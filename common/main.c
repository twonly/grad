#include "config.h"
#include "init.h"
#include "slogger.h"
#include <unistd.h>

#ifndef MFSMAXFILES
#define MFSMAXFILES 5000
#endif

#include "main.h"

#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/time.h>

#include <signal.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <syslog.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <sys/resource.h>
#include <grp.h>
#include <pwd.h>

typedef struct pollentry {
	void (*desc)(struct pollfd *,uint32_t *);
	void (*serve)(struct pollfd *);
	struct pollentry *next;
} pollentry;

static pollentry *pollhead=NULL;

typedef struct deentry {
	void (*fun)(void);
	struct deentry *next;
} deentry;

static deentry *dehead=NULL;


typedef struct timeentry {
	uint32_t nextevent;
	uint32_t seconds;
	uint32_t offset;
	int mode;
//	int offset;
	void (*fun)(void);
	struct timeentry *next;
} timeentry;

static timeentry *timehead=NULL;

static uint32_t now;
static uint64_t usecnow;

volatile int exiting;

void main_pollregister (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *)) {
	pollentry *aux=(pollentry*)malloc(sizeof(pollentry));
	passert(aux);
	aux->desc = desc;
	aux->serve = serve;
	aux->next = pollhead;
	pollhead = aux;
}

void main_destructregister (void (*fun)(void)) {
	deentry *aux=(deentry*)malloc(sizeof(deentry));
	passert(aux);
	aux->fun = fun;
	aux->next = dehead;
	dehead = aux;
}

void* main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void)) {
	timeentry *aux;
	if (seconds==0 || offset>=seconds) {
		return NULL;
	}
	aux = (timeentry*)malloc(sizeof(timeentry));
	passert(aux);
	aux->nextevent = ((now / seconds) * seconds) + offset;
	while (aux->nextevent<now) {
		aux->nextevent+=seconds;
	}
	aux->seconds = seconds;
	aux->offset = offset;
	aux->mode = mode;
	aux->fun = fun;
	aux->next = timehead;
	timehead = aux;
	return aux;
}

void free_all_registered_entries(void) {
	deentry *de,*den;
	pollentry *pe,*pen;
	timeentry *te,*ten;

	for (de = dehead ; de ; de = den) {
		den = de->next;
		free(de);
	}

	for (pe = pollhead ; pe ; pe = pen) {
		pen = pe->next;
		free(pe);
	}

	for (te = timehead ; te ; te = ten) {
		ten = te->next;
		free(te);
	}
}

uint32_t main_time() {
	return now;
}

uint64_t main_utime() {
	return usecnow;
}

void destruct() {
	deentry *deit;
	for (deit = dehead ; deit!=NULL ; deit=deit->next ) {
		deit->fun();
	}
}

void mainloop() {
	uint32_t prevtime = 0;
	struct timeval tv;
	pollentry *pollit;
	struct pollfd pdesc[MFSMAXFILES];
	timeentry *timeit;
	uint32_t ndesc;
	int i;

	while (!exiting){
		ndesc=0;
		for (pollit = pollhead ; pollit != NULL ; pollit = pollit->next) {
			pollit->desc(pdesc,&ndesc);
		}

		i = poll(pdesc,ndesc,50);
		gettimeofday(&tv,NULL);
		usecnow = tv.tv_sec;
		usecnow *= 1000000;
		usecnow += tv.tv_usec;
		now = tv.tv_sec;

		if (i<0) {
			if (errno==EAGAIN) {
				syslog(LOG_WARNING,"poll returned EAGAIN");
				usleep(100000);
				continue;
			}
			if (errno!=EINTR) {
				syslog(LOG_WARNING,"poll error: %s",strerr(errno));
				break;
			}
		} else {
			for (pollit = pollhead ; pollit != NULL ; pollit = pollit->next) {
				pollit->serve(pdesc);
			}
		}

		if (now<prevtime) {
			// time went backward !!! - recalculate "nextevent" time
			// adding previous_time_to_run prevents from running next event too soon.
			for (timeit = timehead ; timeit != NULL ; timeit = timeit->next) {
				uint32_t previous_time_to_run = timeit->nextevent - prevtime;
				if (previous_time_to_run > timeit->seconds) {
					previous_time_to_run = timeit->seconds;
				}
				timeit->nextevent = ((now / timeit->seconds) * timeit->seconds) + timeit->offset;
				while (timeit->nextevent <= now+previous_time_to_run) {
					timeit->nextevent += timeit->seconds;
				}
			}
		} else if (now>prevtime+3600) {
			// time went forward !!! - just recalculate "nextevent" time
			for (timeit = timehead ; timeit != NULL ; timeit = timeit->next) {
				timeit->nextevent = ((now / timeit->seconds) * timeit->seconds) + timeit->offset;
				while (now >= timeit->nextevent) {
					timeit->nextevent += timeit->seconds;
				}
			}
		}

		for (timeit = timehead ; timeit != NULL ; timeit = timeit->next) {
			if (now >= timeit->nextevent) {
				if (timeit->mode == TIMEMODE_RUN_LATE) {
					while (now >= timeit->nextevent) {
						timeit->nextevent += timeit->seconds;
					}
					timeit->fun();
				} else { /* timeit->mode == TIMEMODE_SKIP_LATE */
					if (now == timeit->nextevent) {
						timeit->fun();
					}
					while (now >= timeit->nextevent) {
						timeit->nextevent += timeit->seconds;
					}
				}
			}
		}

		prevtime = now;
	}
}

int initialize(void) {
	uint32_t i;
	int ok;
	ok = 1;
	for (i=0 ; (long int)(RunTab[i].fn)!=0 && ok ; i++) {
		now = time(NULL);
		if (RunTab[i].fn()<0) {
			mfs_arg_syslog(LOG_ERR,"init: %s failed !!!",RunTab[i].name);
			ok=0;
		}
	}
	return ok;
}

int initialize_late(void) {
	uint32_t i;
	int ok;
	ok = 1;
	for (i=0 ; (long int)(LateRunTab[i].fn)!=0 && ok ; i++) {
		now = time(NULL);
		if (LateRunTab[i].fn()<0) {
			mfs_arg_syslog(LOG_ERR,"init: %s failed !!!",RunTab[i].name);
			ok=0;
		}
	}
	now = time(NULL);
	return ok;
}

/* signals */

static int termsignal[]={
	SIGTERM,
  SIGINT,
	-1
};

static int ignoresignal[]={
	SIGQUIT,
#ifdef SIGPIPE
	SIGPIPE,
#endif
#ifdef SIGTSTP
	SIGTSTP,
#endif
#ifdef SIGTTIN
	SIGTTIN,
#endif
#ifdef SIGTTOU
	SIGTTOU,
#endif
#ifdef SIGINFO
	SIGINFO,
#endif
#ifdef SIGUSR1
	SIGUSR1,
#endif
#ifdef SIGUSR2
	SIGUSR2,
#endif
#ifdef SIGCHLD
	SIGCHLD,
#endif
#ifdef SIGCLD
	SIGCLD,
#endif
	-1
};

void termhandle(int signo){
  exiting = 1;
}

void set_signal_handlers(){
  struct sigaction sa;
  uint32_t i;

#ifdef SA_RESTART
  sa.sa_flags = SA_RESTART;
#else
  sa.sa_flags = 0;
#endif
  sigemptyset(&sa.sa_mask);

  sa.sa_handler = termhandle;
  for (i=0 ; termsignal[i]>0 ; i++) {
    sigaction(termsignal[i],&sa,(struct sigaction *)0);
  }

  sa.sa_handler = SIG_IGN;
  for (i=0 ; ignoresignal[i]>0 ; i++) {
    sigaction(ignoresignal[i],&sa,(struct sigaction *)0);
  }
}


int main(int argc,char **argv) {
	char *wrkdir;
	int ch;
	int32_t nicelevel;
	struct rlimit rls;

  exiting = 0;
	strerr_init();
  set_signal_handlers();

	if (initialize()) {
		if (getrlimit(RLIMIT_NOFILE,&rls)==0) {
			syslog(LOG_NOTICE,"open files limit: %lu",(unsigned long)(rls.rlim_cur));
		}

		if (initialize_late()) {
			mainloop();
			ch=0;
		} else {
			ch=1;
		}
	} else {
		fprintf(stderr,"error occured during initialization - exiting\n");
		ch=1;
	}
	destruct();
	free_all_registered_entries();
	strerr_term();

	return ch;
}
