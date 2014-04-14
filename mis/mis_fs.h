#ifndef __MDS_FS_H__
#define __MDS_FS_H__

#include "ppfile.h"
#include "ppds.h"

#include <syslog.h>
#include <stdlib.h>
#include <string.h>

#define HASHSIZE 1007

#define DUMP_FILE "./mds_fs.dump"

hashnode* tab[HASHSIZE];
hashnode* nodetab[HASHSIZE];

int init_fs();
void term_fs();

void add_ppnode(ppnode*);
void remove_ppnode(ppnode*);
void add_file(ppfile*);
void remove_file(ppfile*);

void update_visit(ppfile* f);
void update_visit_all();

ppfile* lookup_file(char*);
ppnode* lookup_ppnode(char*);
//void remove_childnode(ppnode* parent,ppnode* child){

void pickle(char* path);
void unpickle(char* path);

#endif
