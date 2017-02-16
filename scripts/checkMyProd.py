#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import glob
import json
import argparse
import re

# import CRAB3 stuff
import CRABClient

# import CMSSW stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE,'bin', SCRAM_ARCH))

# Add default ingrid storm package
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

# import SAMADhi stuff
from SAMADhi import Dataset, Sample, File, DbStore

from cp3_llbb.GridIn import utils

def get_sample(sample):
    dbstore = DbStore()
    resultset = dbstore.find(Sample, Sample.name==sample)
    return list(resultset.values(Sample.sample_id))

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Babysit-helper for CRAB3 jobs')
    parser.add_argument('--new', action='store_true', help='Start monitoring a new production', dest='new')
    parser.add_argument('-j', '--json', type=str, action='store', dest='outjson', default='prod_default.json',
                        help='json file storing the status of your on-going production') 
    options = parser.parse_args()
    return options

def main():
    #####
    # Initialization
    #####
    options = get_options()
    alltasks = [t for t in os.listdir('tasks') if os.path.isdir(os.path.join('tasks', t))]
    assert len(alltasks) > 0, "No task to monitor in the tasks/ directory"
        
    tasks = {}
    # CRAB3 status
    tasks['COMPLETED'] = []
    tasks['SUBMITFAILED'] = []
    tasks['RESUBMITFAILED'] = []
    tasks['NEW'] = []
    tasks['SUBMITTED'] = []
    tasks['TORESUBMIT'] = []
    tasks['UNKNOWN'] = []
    tasks['QUEUED'] = []
    tasks['FAILED'] = []
    tasks['KILLED'] = []
    tasks['HOLDING'] = []
    # GRIDIN status
    tasks['GRIDIN-INDB'] = []
    
    FWHash = ""
    AnaRepo = ""
    AnaHash = ""

    #####
    # Figure out what is the name of the file things should be written into
    #####
    outjson = options.outjson
    if options.new:
        # NB: assumes all the on-going tasks are for the same analyzer
        module = utils.load_request('tasks/' + alltasks[0])
        psetName = module['OriginalConfig'].JobType.psetName
        print "##### Figure out the code(s) version"
        # first the version of the framework
        FWHash, FWRepo, FWUrl = utils.getGitTagRepoUrl( os.path.join(CMSSW_BASE, 'src/cp3_llbb/Framework') )
        # then the version of the analyzer
        AnaHash, AnaRepo, AnaUrl = utils.getGitTagRepoUrl( os.path.dirname( psetName ) )
        outjson = 'prod_' + FWHash + '_' + AnaRepo + '_' + AnaHash + '.json'
        print "The output json will be:", outjson
    else:
        newestjson = max(glob.iglob('prod_*.json'), key=os.path.getctime)
        if outjson == 'prod_default.json' and newestjson != 'prod_default.json':
            outjson = newestjson
            FWHash, AnaRepo, AnaHash = outjson.strip('prod_').strip('.json').split('_')

    #####
    # Read the json if it exists, then check if COMPLETED samples have been entered in SAMADhi since the script was last run
    #####
    data = {}
    if os.path.isfile(outjson):
        with open(outjson) as f:
            data = json.load(f)
    
        for t in data[u'COMPLETED']:
            if t in data[u'GRIDIN-INDB']:
                continue
            s = re.sub('crab_', '', str(t)) + '_' + FWHash + '_' + AnaRepo + '_' + AnaHash
            s_id = get_sample(unicode(s))
            if len(s_id) > 0:
                data['GRIDIN-INDB'].append(t)
    
    #####
    # Loop over the tasks and perform a crab status
    #####
    for task in alltasks:
        if len(data) > 0 and unicode(task) in data[u'GRIDIN-INDB']:
            tasks['GRIDIN-INDB'].append(task)
            continue
        taskdir = os.path.join('tasks/', task)
        print ""
        print "#####", task, "#####"
        try:
            status = utils.send_crab_command('status', dir = taskdir)
        except CRABClient.ClientExceptions.CachefileNotFoundException:
            print("Something went wrong: directory {} was not properly created. Will count it as 'SUBMITFAILED'...\n".format(taskdir))
            tasks['SUBMITFAILED'].append(task)
            continue
        #except httplib.HTTPException:
        #    print("HTTP error when requesting status for task {}. Trying again.\n".format(taskdir))
        #    status = crabCommand('status', dir = taskdir)
    # {'status': 'COMPLETED', 'schedd': 'crab3-3@submit-4.t2.ucsd.edu', 'saveLogs': 'T', 'jobsPerStatus': {'finished': 1}, 'jobs': {'1': {'State': 'finished'}}, 'publication': {'disabled': []}, 'taskWarningMsg': [], 'publicationFailures': {}, 'outdatasets': None, 'statusFailureMsg': '', 'taskFailureMsg': '', 'failedJobdefs': 0, 'ASOURL': 'https://cmsweb.cern.ch/couchdb', 'totalJobdefs': 0, 'jobSetID': '151022_173830:obondu_crab_HWminusJ_HToWW_M125_13TeV_powheg_pythia8_MiniAODv2', 'jobdefErrors': [], 'collector': 'cmssrv221.fnal.gov,vocms099.cern.ch', 'jobList': [['finished', 1]]}

        status_code = status['status']
        if 'failed' in status['jobsPerStatus']:
            status_code = 'TORESUBMIT'
        tasks[status_code].append(task)
    
    #####
    # Dump the crab status into the output json file
    #####
    with open(outjson, 'w') as f:
        json.dump(tasks, f)
    
    #####
    # Print summary
    #####
    print "##### ##### Status summary (" + str(len(alltasks)), " tasks) ##### #####"
    for key in tasks:
        if len(tasks[key]) == 0:
            continue
        line = key + ": " + str(len(tasks[key]))
        print line
    
    #####
    # Suggest some actions depending on the crab status
    #    * COMPLETED -> suggest the runPostCrab.py command
    #    * SUBMITFAILED -> suggest to rm -r the task and submit again
    #####
    print "##### ##### Suggested actions ##### #####"
    if len(tasks['COMPLETED']) > 0:
        print "##### COMPLETED tasks #####"
        for task in tasks['COMPLETED']:
            print "runPostCrab.py tasks/" + task
    if len(tasks['SUBMITFAILED']) > 0:
        print "##### SUBMITFAILED tasks #####"
        for task in tasks['SUBMITFAILED']:
            print "rm -r tasks/" + task + "; crab submit " + task + ".py"
    if len(tasks['FAILED']) > 0:
        print "##### FAILED tasks #####"
        for task in tasks['FAILED']:
            print "crab resubmit tasks/" + task
    if len(tasks['TORESUBMIT']) > 0:
        print "##### TORESUBMIT tasks #####"
        for task in tasks['TORESUBMIT']:
            print "crab resubmit tasks/" + task + " --siteblacklist=T2_UK_SGrid_RALPP,T1_US_FNAL"

if __name__ == '__main__':
    main()
