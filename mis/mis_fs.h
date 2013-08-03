#ifndef __MDS_FS_H__
#define __MDS_FS_H__

#include "ppfile.h"
#include "ppds.h"

#include <stdlib.h>
#include <string.h>

#define HASHSIZE 3571

#define DUMP_FILE "./mds_fs.dump"

int init_fs();
void term_fs();

void add_file(ppfile*);
void remove_file(ppfile*);

ppfile* lookup_file(char*);

void pickle(char* path);
void unpickle(char* path);

#endif
