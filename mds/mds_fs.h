#ifndef __MDS_FS_H__
#define __MDS_FS_H__

#include "ppfile.h"
#include "ppds.h"

#include <stdlib.h>
#include <string.h>

#define HASHSIZE 1007

int init_fs();
void term_fs();

void add_file(ppfile*);
void remove_file(ppfile*);

ppfile* lookup_file(char*);

#endif
