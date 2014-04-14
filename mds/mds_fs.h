#ifndef __MDS_FS_H__
#define __MDS_FS_H__

#include "ppfile.h"
#include "ppds.h"

#include <stdlib.h>
#include <string.h>

#define HASHSIZE 1007

#define DUMP_FILE "./mds_fs.dump"

hashnode* tab[HASHSIZE];
hashnode* reptab[HASHSIZE];

int init_fs();
void term_fs();

void add_file(ppfile*);
void remove_file(ppfile*);

void add_rep(pprep*);
void remove_rep(pprep*);

ppfile* lookup_file(char*);
pprep* lookup_rep(char*);

void pickle(char* path);
void unpickle(char* path);

#endif
