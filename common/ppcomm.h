#ifndef __PPCOMM_H__
#define __PPCOMM_H__

#define CLTOMD_GETATTR 0x1000
#define MDTOCL_GETATTR 0x1001

#define CLTOMD_ACCESS 0x1002
#define MDTOCL_ACCESS 0x1003

#define CLTOMD_OPENDIR 0x1004
#define MDTOCL_OPENDIR 0x1005

#define CLTOMD_READDIR
#define MDTOCL_READDIR

#define CLTOMD_MKDIR
#define MDTOCL_MKDIR

#define CLTOMD_RELEASEDIR
#define MDTOCL_RELEASEDIR

#define CLTOMD_RMDIR
#define MDTOCL_RMDIR

//#define CLTOMD_CREATE
//#define MDTOCL_CREATE

//#define CLTOMD_OPEN
//#define MDTOCL_OPEN

//#define CLTOMD_READ
//#define MDTOCL_READ

//#define CLTOMD_WRITE
//#define MDTOCL_WRITE

//#define CLTOMD_RELEASE
//#define MDTOCL_RELEASE

#define CLTOMD_RENAME
#define MDTOCL_RENAME

#define CLTOMD_CHMOD
#define MDTOCL_CHMOD

#define CLTOMD_CHOWN
#define MDTOCL_CHOWN

#define CLTOMD_CHGRP
#define MDTOCL_CHGRP

#define CLTOMD_UNLINK
#define MDTOCL_UNLINK

//======================================================================================

#define MDTOMI_GETATTR 0x2001
#define MITOMD_GETATTR 0x2002

#define MDTOMI_ACCESS 0x2003
#define MITOMD_ACCESS 0x2004

#define MDTOMI_OPENDIR 0x2005
#define MITOMD_OPENDIR 0x2006

#define MDTOMI_READDIR 0x2007
#define MITOMD_READDIR 0x2008

#define MDTOMI_MKDIR 0x2009
#define MITOMD_MKDIR 0x2010

#define MDTOMI_RELEASEDIR
#define MITOMD_RELEASEDIR

#define MDTOMI_RMDIR
#define MITOMD_RMDIR

//#define MDTOMI_CREATE
//#define MITOMD_CREATE

//#define MDTOMI_OPEN
//#define MITOMD_OPEN

//#define MDTOMI_RELEASE
//#define MITOMD_RELEASE

#define MDTOMI_RENAME
#define MITOMD_RENAME

#define MDTOMI_CHMOD
#define MITOMD_CHMOD

#define MDTOMI_CHOWN
#define MITOMD_CHOWN

#define MDTOMI_CHGRP
#define MITOMD_CHGRP

#define MDTOMI_UNLINK
#define MITOMD_UNLINK

//===============================================================

#define ANTOAN_NOOP 0x0001

//===============================================================

#define MDTOCS_xxx
#define CSTOMD_xxx

#define CLTOCS_xxx
#define CSTOCL_xxx

#define HEADER_LEN 8

typedef struct ppacket{
  int size;
  int cmd;
  char* buf;

  char* startptr;
  int bytesleft;

  struct ppacket* next;
} ppacket;

ppacket* createpacket_s(int size,int cmd);
ppacket* createpacket_r(int size,int cmd);

#endif
