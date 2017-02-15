#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

import argparse
import os
import sys
import subprocess
import json
import tarfile
import contextlib
import re
from pwd import getpwuid
from os import listdir
from os.path import join, isfile, isdir, dirname
import datetime as dt

# import SAMADhi stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE,'bin', SCRAM_ARCH))

# Add default ingrid storm package
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

from SAMADhi import Dataset, Sample, File, DbStore

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    username = getpwuid(os.stat(os.getcwd()).st_uid).pw_name
    parser = argparse.ArgumentParser(description='Provide a list of things to be deleted in /storage/data/cms/store/user/')
    parser.add_argument('--crabUsername', action='store', dest='crabUsername', default=username, type=str,
        help='crab / storage username')
    parser.add_argument('--ingridUsername', action='store', dest='ingridUsername', default=username, type=str,
        help='ingrid username')
    options = parser.parse_args()
    return options

def get_dataset(inputDataset = None, inputID = None):
    dbstore = DbStore()
    if inputDataset is not None:
        resultset = dbstore.find(Dataset, Dataset.name == inputDataset)
    elif inputID is not None:
        resultset = dbstore.find(Dataset, Dataset.dataset_id == inputID)
    return list(resultset.values(Dataset.name, Dataset.dataset_id, Dataset.nevents, Dataset.process))

def main(crabUsername, ingridUsername):
    dbstore = DbStore()

    print "##### Get the list of potential DB samples of interest"
    list_allDBsamples = []
    results = dbstore.find(Sample)
    for r in results:
        if r.author is None:
            continue
        for f in r.files:
            if crabUsername in f.lfn:
                p = '/storage/data/cms' + re.sub('/output.*root', '', f.lfn)
                if p not in list_allDBsamples:
                    list_allDBsamples.append(p)
        if crabUsername in r.path or ingridUsername in r.author:
            if r.path == '':
                continue
            if r.path not in list_allDBsamples:
                list_allDBsamples.append(r.path)
#            print r.path
    print ""

    storageDir = join('/storage/data/cms/store/user/', crabUsername)
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
                try:
                    ttask = int(taskStamp.replace('_', ''))
                except ValueError:
                    print("Warning: could not interpret path {}, skipping it...".format(taskStamp))
                    continue
                if ttask >= tcut:
                    continue
                for taskID in listdir(join(storageDir, d, subd, taskStamp)):
                    if not isdir(join(storageDir, d, subd, taskStamp, taskID)):
                        continue
                    myPath = join(storageDir, d, subd, taskStamp, taskID)
                    if myPath in list_allDBsamples:
                        continue
#                    print isFramework(myPath), myPath
                    try:
                        mySize = subprocess.check_output(["du", '-s', myPath]).split()[0].decode('utf-8')
                    except subprocess.CalledProcessError:
                        print("Error while accessing file in path {}, skipping it!".format(myPath))
                        continue
                    list_allUserDirs[ttask] = {'path': myPath, 'size': int(mySize) * 1024, 'is CP3-llbb': isFramework(myPath)}

    print '# Tasks older than 6 months'
    print '# timestamp= ', getDateMinusT(currentTime, month = 6)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if t < getDateMinusT(currentTime, month = 6) and list_allUserDirs[t]['is CP3-llbb']:
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint

    print '# Tasks between 3 and 6 months old'
    print '# timestamp= ', getDateMinusT(currentTime, month = 3)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if getDateMinusT(currentTime, month = 6) < t < getDateMinusT(currentTime, month = 3) and list_allUserDirs[t]['is CP3-llbb']:
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint

    print '# Tasks between 1 and 3 months old'
    print '# timestamp= ', getDateMinusT(currentTime, month = 1)
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if getDateMinusT(currentTime, month = 3) < t < getDateMinusT(currentTime, month = 1) and list_allUserDirs[t]['is CP3-llbb']:
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\nrm -r %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
    print '# totalSize= ', sizeof_fmt(totalSize)
    print finalprint

    print '# The following tasks could not be asserted to be cp3_llbb framework tasks or not... deal with them as you see fit:'
    totalSize = 0
    finalprint = ''
    for t in list_allUserDirs:
        if not list_allUserDirs[t]['is CP3-llbb']:
            totalSize += list_allUserDirs[t]['size']
            finalprint += "# size= %s\tpath= %s\n" % (sizeof_fmt(list_allUserDirs[t]['size']), list_allUserDirs[t]['path'])
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
# Taken from http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def isFramework(path, f = 'log/cmsRun_1.log.tar.gz'):
    # Resurrecting some parsing code last seen in runPostCrab
    # https://github.com/cp3-llbb/GridIn/commit/2c5b8b07b30206688d87dafb3b0a9dbfb61e71c7
#    print path, f
    tarLog = join(path, f)
    if not isfile(tarLog):
#        print "\t", isdir(dirname(tarLog)), dirname(tarLog)
        if isdir(dirname(tarLog)):
            logs = [x for x in listdir(dirname(tarLog)) if isfile(join(dirname(tarLog),x))]
            return isFramework(dirname(tarLog), f = logs[0])
        else:
            if 'failed' not in f:
                # maybe the log does not exist because all tasks ran and failed ?
                return isFramework(path, f = 'failed/log/cmsRun_1.log.tar.gz')
            else:
                # impossible to assert if this is a FW task
                return False
    isFW = False
    with tarfile.open(tarLog) as tar:
        for tarFile in tar.getmembers():
            if 'stdout' not in tarFile.name:
                continue
            # For some reason, even though we are using python 2.7, the with statement here seems broken... Using contextlib to handle the file opening / reading cleanly
            with contextlib.closing(tar.extractfile(tarFile)) as file:
                for line in file:
                    if ('cp3_llbb/Framework' in line
                        or 'HHAnalysis' in line or 'ZAAnalysis' in line or 'TTAnalysis' in line
                        or 'hh_analyzer' in line or 'za_analyzer' in line or 'tt_analyzer' in line):
                        isFW = True
                        break
        return isFW

if __name__ == '__main__':
    options = get_options()
    main(options.crabUsername, options.ingridUsername) 
