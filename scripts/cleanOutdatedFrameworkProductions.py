#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

import argparse
import os
import sys
import subprocess
from pwd import getpwuid
from os import listdir
from os.path import join, isfile, isdir, dirname
import shutil
import requests
import re

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
    parser.add_argument('--debug', action='store_true', help='debug mode: running on only one production and do not delete anything', dest='debug')
    parser.add_argument('-s', '--evaluateSize', action='store_true', help='evaluate disk size that would be freed', dest='evaluateSize')
    options = parser.parse_args()
    return options

def main(crabUsername, ingridUsername, DEBUG = False, evaluateSize = False):
    if DEBUG:
        print "RUNNING IN DEBUG MODE"
        print "Nothing will be deleted\n"

    dbstore = DbStore()

    print "##### Get the list of potential DB samples of interest"
    list_allDBsamples = []
    results = dbstore.find(Sample)
    for r in results:
        if crabUsername in r.path:
            list_allDBsamples.append([r.name, r.source_dataset_id])
    print ""

    print "##### Get the list of existing productions"
    # before anything else: get the list of tags to not touch
    whitelist = requests.get('https://raw.githubusercontent.com/cp3-llbb/GridIn/master/data/SAMADhi_doNOTdelete_whitelist.json').json()
    if DEBUG:
        print "production whitelist= ", whitelist
    list_all_productions = []
    for i, s in enumerate(list_allDBsamples):
        s_name, s_id = s
        isProdAlreadyListed = False
        isSampleProtected = False
        for FWtag, Anatag in list_all_productions:
            if FWtag in str(s_name) and Anatag in str(s_name):
#                print "This prod is already in the list, FWtag= ", FWtag, "Anatag= ", Anatag
                isProdAlreadyListed = True
                break
        if isProdAlreadyListed:
            continue
        tags = str(s_name)
        # Get the tags: 
        # First of all: check if the sample is protected or not
        for ana in whitelist:
            part = str(ana)
            for protectedtag in whitelist[ana]:
                t = str(protectedtag).split('_%s_' % part)
                if t[0] in tags and t[1] in tags:
                    if DEBUG:
                        print '\tSkipping whitelisted sample %s' % s_name
                    isSampleProtected = True
            if not isSampleProtected:
                tags = tags.replace(part, '') # remove HHAnalyzer and the like from the name of the sample
        if isSampleProtected:
            continue
        # now extract the fw and analyzer tags
        # for analyzer, this is always the last part of the sample name so we don't have to worry about naming conventions there (fortunately)
        tags = tags.split('_')
        Anatag = tags[-1]
        tags = tags[:-1]
        # for FW the following regex should work ((v\d+.\d+.\d+\+\d+X?)(-\d+-g[0-9a-f]{7,40})?)|([0-9a-f]{7,40})
        # it matches either:
        #   - a framework tag (possibly with a final X): v1.2.0+7415
        #   - possibly followed by a number of commits and a 'g' plus 7 to 40 characters git hash: v1.2.0+7415-79-ga5b16ff
        #   - or alternatively a 7 to 40 characters git hash: f2f0a44
        tags = [x for x in tags if re.match('((v\d+.\d+.\d+\+\d+X?)(-\d+-g[0-9a-f]{7,40})?)|([0-9a-f]{7,40})', x)]
        if DEBUG:
            print tags, Anatag
        if len(tags) != 1:
            print "ERROR, there are spurious things in the sample name, please figure out what is happening:"
            print "FWtags= ", tags
            return 1
        FWtag = tags[0]
        list_all_productions.append([FWtag, Anatag])

    for i, p in enumerate(list_all_productions):
        if DEBUG and i > 0:
            break
        FWtag, Anatag = p

        extrastring = ''
        if not evaluateSize:
            extrastring = '(evaluation of the disk size is OFF by default)'
        print "\n##### Now looking at prod FWtag= ", FWtag, 'Anatag= ', Anatag, 'and list the associated folders %s' % extrastring
        totalSize = 0
        totalSamples = 0
        cannotManageToDeleteThisProd = False
        for s_name, s_id in list_allDBsamples:
            if FWtag in str(s_name) and Anatag in str(s_name):
                result = dbstore.find(Sample, Sample.name == s_name)
                s = result.one()
                if evaluateSize:
                    totalSize += int(subprocess.check_output(["du", '-s', str(s.path)]).split()[0].decode('utf-8'))
                totalSamples += 1
                if s.source_sample is not None:
                    print "WARNING, the sample", s_name, "depend on another sample, aborting now"
                    cannotManageToDeleteThisProd = True
                    break
                if s.derived_samples.count() > 0:
                    print "WARNING, the sample", s_name, "has derived samples, aborting now"
                    cannotManageToDeleteThisProd = True
                    break
                if s.results.count() > 0:
                    print "WARNING, the sample", s_name, "has derived results, aborting now"
                    cannotManageToDeleteThisProd = True
                    break
                print s.path
        if cannotManageToDeleteThisProd:
            continue

        print '\tFWtag= ', FWtag, 'Anatag= ', Anatag, 'totalSamples= ', totalSamples, 'totalSize= ', totalSize, "(%s)" % sizeof_fmt(totalSize)
        if confirm(prompt='\tDo you REALLY want to DELETE this prod from disk and from SAMADhi?', resp=False):
            for s_name, s_id in list_allDBsamples:
                if FWtag in str(s_name) and Anatag in str(s_name):
                    result = dbstore.find(Sample, Sample.name == s_name)
                    s = result.one()
                    if DEBUG:
                        print 'rm -r %s' % s.path
                        print 'rm -r %s' % str(s.path).rsplit('/0000', 1)[0]
                        print 'dbstore.remove()'
                    else:
                        try:
                            shutil.rmtree(s.path)
                            shutil.rmtree(str(s.path).rsplit('/0000', 1)[0])
                        except OSError:
                            print "Seems we have a buggy path: %s" % s.path
                            print "deleting the DB entry then moving on..."
                        dbstore.remove(s)
                        dbstore.commit()

# FIXME: deal with extensions and merged samples

def sizeof_fmt(num, suffix='B'):
# Taken from http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ['','k','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Y', suffix)

def confirm(prompt=None, resp=False):
# shamelessly taken from SAMADhi/scripts/userPrompt.py
    """prompts for yes or no response from the user. Returns True for yes and
    False for no. 'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.
    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True
    """
    if prompt is None:
        prompt = 'Confirm'
    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')
    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print 'please enter y or n.'
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

if __name__ == '__main__':
    options = get_options()
    main(options.crabUsername, options.ingridUsername, DEBUG = options.debug, evaluateSize = options.evaluateSize) 
