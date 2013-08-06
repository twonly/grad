#!/usr/bin/env python

import re
import sys
from pdb import set_trace

def init_table(prefix,dev):
    filename = prefix + "." + str(dev)
    try:
        f = open(filename,"r")
    except IOError:
        return None

    tab = {}
    for s in f.readlines():
        s = s[:-1]
        dirid,fid,pfid = re.split("[ \t]+",s)
        dirid = int(dirid)
        fid = int(fid)
        pfid = int(pfid)

        tab[fid] = [dirid,pfid]

    return tab

def full_path(tab,fid):
    if tab.get(fid,None) == None:
        return None

    dirid,pfid = tab[fid]

    #print("fid=%d,dirid=%d,pfid=%d" % (fid,dirid,pfid))
    if pfid == fid:
        return "/",pfid
    else:
        ppath,_ = full_path(tab,pfid)
        if ppath == None:
            return None

        if dirid == 0:
            if ppath == "/":
                ret = ppath + str(fid)
            else:
                ret = ppath + "/" + str(fid)
        else:
            if ppath == "/":
                ret = ppath + str(dirid)
            else:
                ret = ppath + "/" + str(fid)

        return ret,pfid

def test1():
    return init_table("./dirdb.res/dirgroup.19970430",9)

def test2(dev,fid):
    tab = init_table("./dirdb.res/dirgroup.19970430",dev)
    print("%d => %s" % (dev,full_path(tab,fid)))

if __name__ == "__main__":
    dev = int(sys.argv[1])
    fid = int(sys.argv[2])

    test2(dev,fid)
