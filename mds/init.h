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

#include <stdio.h>

#include "random.h"
#include "mds.h"
#include "mds_fs.h"
#include "mdscs.h"
#include "chunks.h"

/* Run Tab */
typedef int (*runfn)(void);
struct {
	runfn fn;
	char *name;
} RunTab[]={
	{rnd_init,"random generator"},
  {chunks_init,"mds chunks init"},
  {init_fs, "mds_fs init"},
  {mds_init,"mds init"},
  {mdscs_init,"mdscs init"},
	{(runfn)0,"****"}
},LateRunTab[]={
	{(runfn)0,"****"}
};
