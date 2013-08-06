#!/usr/bin/env python

import query_dir
import re
from pdb import set_trace

dirdb_prefix = "./dirdb.res/dirgroup.19970430"
trace_prefix = "./traces/trace"

dev_dirtab = {}

def init_dev_dirtab(dev):
    global dirdb_prefix

    dev_dirtab[dev] = query_dir.init_table(dirdb_prefix,dev)

def get_full_path(dev,fid):
    global dev_dirtab
    dev = abs(int(dev))

    if dev_dirtab.get(dev,None) == None:
       init_dev_dirtab(dev)

    try:
        ret = query_dir.full_path(dev_dirtab[dev],abs(int(fid)))
    except ValueError:
        return None

    if ret == None:
        return None

    return ret[0]


def parse_call(line):
    ret = {}
    ps = re.split(",[ \t]+",line)
    for pair in ps:
        tmp = re.split("=",pair)
        if len(tmp) == 2:
            p,v = tmp
            ret[p] = v

    return ret

def print_param(param):
    for p,v in param.items():
        if p != "fid":
            print("%s = %s" % (p,v))
        else:
            dev = int(param["dev"])
            path = get_full_path(dev,v)
            print("%s = %s (path=%s)" % (p,v,path))

def test_inspect():
    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")

    cmd = str(raw_input())
    while cmd != 'q':
        L = f.readline()
        if L == '': break
        L = L[:-1]

        param = parse_call(L)

        print_param(param)

        print("=" * 20)
        cmd = str(raw_input())

def test_call_types():
    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")
    L = f.readline()

    stat = {}
    line = 0
    while L != '':
        L = L[:-1]

        param = parse_call(L)
        sys = param["sys"]
        stat.setdefault(sys,0)
        stat[sys] += 1

        L = f.readline()

        line += 1
        if line % 10000 == 0:
            print(line)

    print(stat)

# Result for 19960911:
#{'RENAME': 973, 'STAT': 804048, 'FTRUNC': 115, 'SMOUNT': 26, 'SYNC': 1692, 'CHOWN': 175, 'LINK': 172, 'FCHDIR': 1066, 'UNLINK': 3218, 'OPEN': 79163, 'FSTAT': 727905, 'EXECVE': 9343, 'CHROOT': 5, 'FSYNC': 5032, 'SREAD': 139224, 'RDLINK': 50, 'GETDIRENTRIES': 21211, 'CLOSE': 80919, 'UTIME': 196, 'FORK': 21242, 'CHDIR': 1989, 'SWRITE': 41982, 'MKDIR': 5, 'GETACL': 2, 'FCHOWN': 96, 'VFORK': 934, 'LOCKF': 3405, 'CREAT': 1242, 'LSTAT': 13848, 'MMAP': 31631, 'REBOOT': 67, 'CHMOD': 1765, 'WRITEV': 776, 'ACCESS': 12644, 'FCHMOD': 882, 'EXIT': 11043, 'TRUNC': 5, 'MUNMAP': 47}

def test_call_path():
    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")
    L = f.readline()

    paths = {}
    line = 0
    while L != '':
        L = L[:-1]

        param = parse_call(L)
        if param.get("fid",None) != None:
            dev = abs(int(param["dev"]))
            sys = param["sys"]
            v = int(param["fid"])
            path = get_full_path(dev,v)

            paths.setdefault((dev,v,path),[])
            paths[(dev,v,path)].append(sys)

        line += 1
        if line % 10000 == 0:
            print(line)

        L = f.readline()

    f.close()
    print("Done parsing")

    f = open("19960911.call_path.txt","w")
    for p,v in paths.items():
        f.write("%s:{\n" % str(p))
        f.write("\t%s\n" % str(v))
        f.write("}\n\n")

def test_get_instance(items):
    items_set = set(items)

    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")
    L = f.readline()

    line = 0
    instances = {}
    while L != '':
        L = L[:-1]

        param = parse_call(L)
        sys = param.get("sys",None)
        if sys in items_set:
            items_set.remove(sys)
            instances[sys] = L

        if len(items_set) == 0:
            break

        L = f.readline()

        line += 1
        if line % 10000 == 0:
            print(line)
            print(items_set)

    for p,v in instances.items():
        print("%s:\n%s" % (p,v))

def test_call_hid_uid(apis):
    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")
    L = f.readline()
    line = 0

    hid_uids = {}
    while L != '':
        L = L[:-1]

        param = parse_call(L)
        hid = param.get("hid",None)
        uid = param.get("uid",None)
        sys = param.get("sys",None)

        if uid != None and sys != None and hid != None and sys in apis:
            hid_uids.setdefault((hid,uid),{})
            hid_uids[(hid,uid)].setdefault(sys,0)

            hid_uids[(hid,uid)][sys] += 1

        line += 1
        if line % 10000 == 0:
            print(line)

        L = f.readline()

    f.close()

    f = open("19960911.call_hid_uid.txt","w")
    for p,v in hid_uids.items():
        f.write("%s:{\n" % str(p))
        for sys,c in v.items():
            f.write("\t%s=%d,\n" % (sys,c))
        f.write("}\n\n")


def test_call_uids(apis):
    global trace_prefix

    f = open(trace_prefix + ".19960911.txt","r")
    l = f.readline()
    line = 0

    uids = {}
    while l != '':
        l = l[:-1]

        param = parse_call(l)
        hid = param.get("hid",None)
        uid = param.get("uid",None)

        if uid != none and hid != none:
            uids.setdefault(hid,set([]))
            uids[hid].add(uid)

        line += 1
        if line % 10000 == 0:
            print(line)

        l = f.readline()

    f.close()

    f = open("19960911.call_uids.txt","w")
    for p,v in uids.items():
        f.write("%s:{\n" % str(p))
        f.write("\t%s\n" % str(v))
        f.write("}\n\n")

def test_call_hid(dates,apis):
    global trace_prefix

    for date in dates:
        print("Reading %s" % str(date))

        f = open(trace_prefix + "." + str(date) + ".txt","r")
        L = f.readline()
        line = 0

        hids = {}
        while L != '':
            L = L[:-1]

            param = parse_call(L)
            hid = param.get("hid",None)
            sys = param.get("sys",None)
            fid = param.get("fid",None)
            dev = param.get("dev",None)

            if  hid != None and fid != None and sys in apis and dev != None:
                full_path = get_full_path(dev,fid)
                if full_path != None:
                    hids.setdefault((dev,fid),[])
                    hids[(dev,fid)].append(hid)

            line += 1
            if line % 10000 == 0:
                print(line)

            L = f.readline()

        f.close()

    f = open("%s.call_hids.txt" % str(dates),"w")
    for p,v in hids.items():
        f.write("%s:{\n" % str(p))
        f.write("\t(hids)%s\n" % str(v))
        f.write("}[%d]\n\n" % len(set(v)))

def test_call_file_uids(dates,apis):
    global trace_prefix
    uids = set([])

    for date in dates:
        f = open(trace_prefix + ".%s.txt" % str(date),"r")
        l = f.readline()
        line = 0

        file_uid = {}
        while l != '':
            l = l[:-1]

            param = parse_call(l)
            #hid = param.get("hid",None)
            uid = param.get("uid",None)
            sys = param.get("sys",None)
            fid = param.get("fid",None)
            dev = param.get("dev",None)

            if uid != None and sys in apis and fid != None and dev != None:
                full_path = get_full_path(dev,fid)
                if full_path != None:
                    file_uid.setdefault((dev,fid),[])
                    file_uid[(dev,fid)].append(uid)


            line += 1
            if line % 10000 == 0:
                print(line)

            l = f.readline()

        f.close()

    f = open("%s.call_file_uid.txt" % str(dates),"w")
    for p,v in file_uid.items():
        if len(set(v)) != 1:
            f.write("%s:{\n" % str(p))
            f.write("\t%s\n" % str(v))
            f.write("\t}[%d]\n" % len(set(v)))
            uids = uids.union(set(v))

    f.write("\n"+ "="*10 + "uids%s\n" % uids)

if __name__ == "__main__":
    #test_inspect()
    #test_call_types()
    #test_call_path()

    #sys_types = ['STAT', 'FTRUNC', 'CHOWN', 'LINK', 'UNLINK', 'OPEN', 'FSTAT', 'SREAD', 'GETDIRENTRIES', 'CLOSE', 'UTIME', 'SWRITE', 'MKDIR', 'FCHOWN', 'CREAT', 'LSTAT', 'REBOOT', 'CHMOD', 'WRITEV', 'ACCESS', 'FCHMOD', 'TRUNC']

    sys_types = ['STAT','FSTAT','LSTAT','READ','SREAD','SWRITE','WRITEV','READV']
    dates = []
    dates += [19960911]
    #dates += range(19960911,19960930+1)
    #dates += range(19961001,19961007+1)
    #dates += range(19961101,19961130+1)
    #test_call_hid(dates,sys_types)

    #test_get_instance(sys_types)

    #test_call_hid_uid(sys_types)
    #test_call_uids()

    test_call_file_uids(dates,sys_types)

    '''
    Result for 19960911:
    %#STAT:
    t=842425208, hid=9, uid=109, pid=0, sys=STAT, dev=9, fid=4
    %#CHOWN:
    t=842453796, hid=6, uid=27881, pid=16134, sys=CHOWN, dev=4043, fid=1851
    %#LINK:
    t=842429648, hid=8, uid=15987, pid=9, sys=LINK, dev=8, fid=72
    %#FTRUNC:
    t=842429648, hid=8, uid=15987, pid=9, sys=FTRUNC, dev=8, fid=71, size=768
    %#UNLINK:
    t=842425208, hid=9, uid=176, pid=0, sys=UNLINK, dev=2009, fid=5
    #OPEN:
    t=842425207, hid=9, uid=16192, pid=0, sys=OPEN, dev=9, fid=-9, fd=5, ftyp=FTYPE_DIR, fstp=FSTYPE_NFS, uid=0, siz=1024, nlnk=17, ctm=842372347, mtm=842372347, atm=842421607, mode=RDONLYMODE
    %#FSTAT:
    t=842425207, hid=9, uid=16192, pid=0, sys=FSTAT, dev=-9, fid=-9
    #SREAD:
    t=842425207, hid=9, uid=16197, pid=0, sys=SREAD, dev=9, fid=2, ofst=0, byts=176
    #GETDIRENTRIES:
    t=842425207, hid=9, uid=16192, pid=0, sys=GETDIRENTRIES, dev=-9, fid=-9
    #CLOSE:
    t=842425207, hid=9, uid=16197, pid=0, sys=CLOSE, dev=9, fid=2, cmod=CLOSE_HOST
    %#UTIME:
    t=842429649, hid=8, uid=15995, pid=9, sys=UTIME, dev=8, fid=73
    #SWRITE:
    t=842425208, hid=9, uid=176, pid=0, sys=SWRITE, dev=-9, fid=-9, ofst=-9, byts=45
    %#FCHOWN:
    t=842438481, hid=5, uid=11592, pid=23727, sys=FCHOWN, dev=5, fid=81
    #CREAT:
    t=842425309, hid=7, uid=11091, pid=0, sys=CREAT, dev=2007, fid=4, fd=1, ftyp=FTYPE_REG, fstp=FSTYPE_NFS, uid=0, siz=0, nlnk=1, ctm=842425309, mtm=842425309, atm=842425309, mode=ACCMODE|TRUNCMODE|CREATMODE
    %#CHMOD:
    t=842428811, hid=9, uid=16234, pid=0, sys=CHMOD, dev=9, fid=29
    #WRITEV:
    t=842425321, hid=6, uid=195, pid=0, sys=WRITEV, dev=-9, fid=-9, ofst=-9, byts=70
    #FCHMOD:
    t=842451437, hid=6, uid=10763, pid=16134, sys=FCHMOD, dev=4043, fid=532
    %#TRUNC:
    t=842486202, hid=7, uid=12696, pid=21088, sys=TRUNC, dev=2007, fid=930, size=46462 '''

