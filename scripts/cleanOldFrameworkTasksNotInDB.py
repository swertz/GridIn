#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

import argparse
import os
import sys
import subprocess
import json
from pwd import getpwuid
from os import listdir
from os.path import join, isfile, isdir
import datetime as dt

# import SAMADhi stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE,'bin', SCRAM_ARCH))

# Add default ingrid storm package
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

from SAMADhi import Dataset, Sample, File, DbStore

def get_dataset(inputDataset = None, inputID = None):
    dbstore = DbStore()
    if inputDataset is not None:
        resultset = dbstore.find(Dataset, Dataset.name == inputDataset)
    elif inputID is not None:
        resultset = dbstore.find(Dataset, Dataset.dataset_id == inputID)
    return list(resultset.values(Dataset.name, Dataset.dataset_id, Dataset.nevents, Dataset.process))

def main():
    username = getpwuid(os.stat(os.getcwd()).st_uid).pw_name
    dbstore = DbStore()

    print "##### Get the list of potential DB samples of interest"
    list_allDBsamples = []
    results = dbstore.find(Sample)
    for r in results:
        if username in r.path or username in r.author:
            if r.path == '':
                continue
            list_allDBsamples.append(r.path)
#            print r.path
    print ""

    storageDir = join('/storage/data/cms/store/user/', username)
    print "##### Get the list of user paths in %s" % storageDir

    list_allUserDirs = {}
    currentTime = dt.datetime.now()
    tcut = getDateMinusT(currentTime, month = 1)
    for d in listdir(storageDir):
        if not isdir(join(storageDir, d)):
            continue
        if 'CRAB_PrivateMC' in d or 'testFiles' in d :
            continue
        for subd in listdir(join(storageDir, d)):
            if not isdir(join(storageDir, d, subd)):
                continue
            for taskStamp in listdir(join(storageDir, d, subd)):
                if not isdir(join(storageDir, d, subd, taskStamp)):
                    continue
                ttask = int(taskStamp.replace('_', ''))
                if ttask >= tcut:
                    continue
                for taskID in listdir(join(storageDir, d, subd, taskStamp)):
                    if not isdir(join(storageDir, d, subd, taskStamp, taskID)):
                        continue
                    myPath = join(storageDir, d, subd, taskStamp, taskID)
                    if myPath not in list_allDBsamples:
                        mySize = subprocess.check_output(["du", '-s', myPath]).split()[0].decode('utf-8')
                        list_allUserDirs[ttask] = {'path': myPath, 'size': int(mySize) * 1024}

    print '# Tasks older than 6 months'
    print '# timestamp= ', getDateMinusT(currentTime, month = 6)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if t < getDateMinusT(currentTime, month = 6):
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint

    print '# Tasks between 3 and 6 months old'
    print '# timestamp= ', getDateMinusT(currentTime, month = 3)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if getDateMinusT(currentTime, month = 6) < t < getDateMinusT(currentTime, month = 3):
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint

    print '# Tasks between 1 and 3 months old'
    print '# timestamp= ', getDateMinusT(currentTime, month = 1)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if getDateMinusT(currentTime, month = 3) < t < getDateMinusT(currentTime, month = 1):
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint


def getDateMinusT(currentTime, year = 0, month = 3, day = 0):
    day = day + 365 * year
    day = day + 31 * month
    t = dt.timedelta(day)
    t = currentTime - t
    y = t.year - 2000
    mo = t.month
    d = t.day
    h = t.hour
    mi = t.minute
    s = t.second
    t = [y, mo, d, h, mi, s]
    t = map(str, t)
    t = [x.zfill(2) for x in t]
    t = ''.join(t)
    return int(t)

def sizeof_fmt(num, suffix='B'):
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

if __name__ == '__main__':
    main() 
