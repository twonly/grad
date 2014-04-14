#!/usr/bin/env python
from __future__ import division
import query_dir
import re
import os,sys
import time
import stat
from collections import deque
from pdb import set_trace

dirdb_prefix = "./dirdb.res/dirgroup.19970430"
trace_prefix = "./traces/"

dev_dirtab = {}

do_init_entries = False
mt_hid = 9

for arg in sys.argv[1:]:
    if arg == '-p':
        do_init_entries = True
    if '=' in arg:
        ret = arg.split("=")
        if len(ret) != 2:
            print("Invalid argument %s" % arg)
            sys.exit(1)

        a,b = ret
        if a == "hid":
            mt_hid = int(b)
            print("hid=%d" % mt_hid)
        else:
            print("Unknown parameter %s" % a)
            sys.exit(1)

#mount = "./mt_%d" % mt_hid # point of mount
#mount = "./mt_"
mount = "mnt"

time_scale = 100
thread_count = 10
min_queue_size = 1000

q = deque([])

t = 0 #current time

fds = {}

dates = []
#dates += range(19960911,19960930+1)
#dates += range(19961001,19961031+1)
#dates += range(19961101,19961130+1)
#dates += range(19961201,19961231+1)
#dates += range(19970101,19970131+1)
#dates += range(19970201,19970228+1)
#dates += range(19970301,19970331+1)
#dates += range(19970401,19970430+1)
#dates = [19960911]

#dates += range(19961001,19961015+1)
#dates += [19961001]
dates += ["hid9.stat"]

apis = ["CREAT", "OPEN"]
apis = ["STAT","FSTAT"]
#apis += ["CHOWN","FCHOWN","CHMOD","FCHMOD"]
#apis += ["LINK","UNLINK"]
apis += ["UNLINK"]
#apis += ["FTRUNC",'TRUNC',"OPEN","CLOSE"]
apis += ["GETDIRENTRIES",'UTIME']
#apis += ['SREAD','SWRITE','WRITEV']

#hids = range(1,11+1)

def init_dev_dirtab(dev):
    global dirdb_prefix

    ret = query_dir.init_table(dirdb_prefix,dev)
    if ret != None:
        dev_dirtab[dev] = ret

def get_full_path(dev,fid): #dev+fid = full_path
    global dev_dirtab
    dev = abs(int(dev))

    if dev_dirtab.get(dev,None) == None:
       init_dev_dirtab(dev)
       if dev_dirtab.get(dev,None) == None:
           return None

    ret = query_dir.full_path(dev_dirtab[dev],abs(int(fid)))

    if ret == None:
        return None

    return "/" + str(dev) + ret[0]

def parse_call(line):
    ret = {}
    ps = re.split(",[ \t]+",line)
    for pair in ps:
        tmp = re.split("=",pair)
        if len(tmp) == 2:
            p,v = tmp
            ret[p] = v

    return ret

def init(prefix,date,paths,apaths,npaths):
    global mt_hid

    f = open(prefix + str(date),"r")
    L = f.readline()

    print("reading %s" % date)

    line = 0
    while L != '':
        #L = L.rstrip()

        line += 1
        if line % 10000 == 0:
            print(line)

        #if line > 30000:
        #    break

        L=L[:-1]
        #print L
        param = parse_call(L)
        #print param
        L = f.readline()

        try:
            dev = abs(int(param["dev"]))
            sys = param["syscall"]
            fid = abs(int(param["fid"]))
            hid = int(param["hid"])
        except ValueError:
            continue
        except KeyError:
            continue

        #print dev, sys, fid
        path = get_full_path(dev,fid)
        if path != None and path[:3]=="/9/":
        #    if hid == mt_hid:
          print path
          apaths.add(path)
          if not path in paths: #discovered by host $mt_hid
            npaths.add(path)
          paths.add(path)

    print("apaths len" + str(len(apaths)))
    print("npaths len" + str(len(npaths)))
    print("paths len" + str(len(paths)))
    f.close()

    return paths,apaths,npaths

def create_file(path):
    global mount

    parent = os.path.dirname(path)
    create_dir(parent)

    cmd = "ls %s; touch %s" % (mount + path,mount + path)

    print(cmd)
    os.system(cmd)

def create_dir(path):
    global mount
    exist = True
    #print(path)
    try:
        stat = os.stat(mount + path)
    except:
        exist = False

    if not exist:
        cmd = "mkdir -p %s" % (mount + path)

        #print(cmd)
        os.system(cmd)

def create_init_entries(apaths,npaths):
    #for path in paths:
        #parent = os.path.dirname(path)
        #create_dir(path)

    for path in apaths:
        parent = os.path.dirname(path)
        create_dir(parent)
    print("Dir created")

    for path in npaths:
        create_file(path)
    print("File created")

def play_call(param):
    if param.get("syscall",None) == None:
        return

    sys = param["syscall"]

    def get_dev_fid_path():
        try:
            fid = param["fid"]
            dev = param["dev"]
        except ValueError:
            return None
        except KeyError:
            return None

        path = get_full_path(dev,fid)
        if path == None:
            return None

        path = mount + path

        return dev,fid,path


    if sys == "STAT" or sys == "FSTAT":
        ret = get_dev_fid_path()
        if ret == None:
            return

        dev,fid,path = ret

        try:
            os.stat(path)
        except OSError as e:
            print("stat",str(e))
            return

    elif sys == "UNLINK":
        ret = get_dev_fid_path()
        if ret == None:
            return

        dev,fid,path = ret
        try:
            st = os.stat(path)
            if stat.S_ISDIR(st.st_mode):
                os.rmdir(path)
            else:
                os.unlink(path)
        except OSError as e:
            print("unlink",str(e))

            return

    elif sys == "GETDIRENTRIES":
        ret = get_dev_fid_path()
        if ret == None:
            return

        dev,fid,path = ret
        try:
            os.listdir(path)
        except OSError as e:
            print("getdirentries",str(e))
            return

    elif sys == "UTIME":
        ret = get_dev_fid_path()
        if ret == None:
            return

        dev,fid,path = ret
        try:
            os.utime(path,None)
        except OSError as e:
            print("utime",str(e))
            return

    else:
        pass

if __name__ == "__main__":
    #initialize
    print("Initializing, hid=%d" % mt_hid)

    if do_init_entries == True:
        print("Initializing paths")

        paths = set([])
        apaths = set([])
        npaths = set([])
        for date in dates:
            paths,apaths,npaths = init(trace_prefix,date,paths,apaths,npaths)  #retrieve new file and dir

        print("Creating files and dirs")

        create_init_entries(apaths,npaths) #create dir and file

        sys.exit(1)


    fp = None
    date_index = 0
    line = None

    def get_call():
        global fp,date_index,line
        param = None

        while True:
        #while line <= 30000:
            if fp == None:
                if date_index == len(dates):
                    return None
                print trace_prefix+str(dates[date_index])
                fp = open(trace_prefix + str(dates[date_index]), "r")
                line = 0
                date_index += 1

            L = fp.readline()
            line += 1
            if L == '':
                fp.close()

                if date_index == len(dates):
                    fp = None
                    return None
                fp = open(trace_prefix + str(dates[date_index]), "r")
                line = 0
                date_index += 1

                L = fp.readline()
                line += 1
            #print L
            L = L[:-1]
            param = parse_call(L)
            return param
            #print param
            try:
                sys = param["syscall"]
                hid = int(param["hid"])
            except ValueError:
                param = None
                continue
            except KeyError:
                param = None
                continue

            if (sys in apis) and (abs(hid) == mt_hid):
                #print "find matched hid and api"
                break

        return param

    #time.sleep(20)

    vt0 = 842425207 #time of the first traced call
    t0 = time.time() #begin time, physical time

    print("Initialization finished")

    #start replay
    lat = 0
    calls = 0
    while True:
        while len(q) < min_queue_size:
            param = get_call() #read 1000 lines

            print param
            if param == None:
                break
            #push time value and command
            q.append((int(param["t"]),param))
        #print("read 1000 lines")
        if len(q) == 0:
            print "BREAK"
            break

        while len(q) > 0:
            x = q.popleft()
            p = x[1]
            try:
                dev = abs(int(p["dev"]))
                sys = p["sys"]
                fid = abs(int(p["fid"]))
                hid = int(p["hid"])
            except ValueError:
                continue
            except KeyError:
                continue

            path = get_full_path(dev,fid)
            print path
            create_file(path)

    print("="*10)
    print("Finished replay" + "\n" * 5)
    print("lat = %f" % lat)

