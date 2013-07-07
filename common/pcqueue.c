/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "config.h"

#include <stdlib.h>
#include <pthread.h>
#include <inttypes.h>
#include <errno.h>

#include "massert.h"

typedef struct _qentry {
	uint32_t id;
	struct _qentry *next;
} qentry;

typedef struct _queue {
	qentry *head,**tail;
	uint32_t elements;
	uint32_t size;
	uint32_t freewaiting;
	pthread_cond_t waitfree;
	pthread_mutex_t lock;
} queue;

void* queue_new(){
	queue *q;
	q = (queue*)malloc(sizeof(queue));
	passert(q);
	q->head = NULL;
	q->tail = &(q->head);
	q->elements = 0;
	q->size = 0;
	q->freewaiting = 0;
	zassert(pthread_cond_init(&(q->waitfree),NULL));
	zassert(pthread_mutex_init(&(q->lock),NULL));
	return q;
}

void queue_delete(void *que) {
	queue *q = (queue*)que;
	qentry *qe,*qen;
	zassert(pthread_mutex_lock(&(q->lock)));
	sassert(q->freewaiting==0);
	for (qe = q->head ; qe ; qe = qen) {
		qen = qe->next;
		free(qe);
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	zassert(pthread_mutex_destroy(&(q->lock)));
	zassert(pthread_cond_destroy(&(q->waitfree)));
	free(q);
}

int queue_isempty(void *que) {
	queue *q = (queue*)que;
	int r;
	zassert(pthread_mutex_lock(&(q->lock)));
	r=(q->elements==0)?1:0;
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

uint32_t queue_elements(void *que) {
	queue *q = (queue*)que;
	uint32_t r;
	zassert(pthread_mutex_lock(&(q->lock)));
	r=q->elements;
	zassert(pthread_mutex_unlock(&(q->lock)));
	return r;
}

int queue_put(void *que,uint32_t id){
	queue *q = (queue*)que;
	qentry *qe;
	qe = malloc(sizeof(qentry));
	passert(qe);
	qe->id = id;
	qe->next = NULL;
	zassert(pthread_mutex_lock(&(q->lock)));
	q->elements++;
	*(q->tail) = qe;
	q->tail = &(qe->next);
	if (q->freewaiting>0) {
		zassert(pthread_cond_signal(&(q->waitfree)));
		q->freewaiting--;
	}
	zassert(pthread_mutex_unlock(&(q->lock)));
	return 0;
}

int queue_get(void *que,uint32_t *id){
	queue *q = (queue*)que;
	qentry *qe;
	zassert(pthread_mutex_lock(&(q->lock)));
	if (q->elements==0) {
		zassert(pthread_mutex_unlock(&(q->lock)));
		if (id) {
			*id=0;
		}
		errno = EBUSY;
		return -1;
	}
	qe = q->head;
	q->head = qe->next;
	if (q->head==NULL) {
		q->tail = &(q->head);
	}
	q->elements--;
	zassert(pthread_mutex_unlock(&(q->lock)));
	if (id) {
		*id = qe->id;
	}
	free(qe);
	return 0;
}
