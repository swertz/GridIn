#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

import argparse
import os
import sys
import subprocess
import json
from pwd import getpwuid

# import SAMADhi stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE,'bin', SCRAM_ARCH))

# Add default ingrid storm package
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

from SAMADhi import Dataset, Sample, File, DbStore
import das_import

# import some lumi utils
from FWCore.PythonUtilities.LumiList import LumiList

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Gather the information on a processed sample and insert the information in SAMADhi')
    parser.add_argument('-i', dest='SAMPLE_ID', type=int, metavar='SAMADHI_SAMPLE_ID', nargs='+',
                        help='SAMADhi sample IDs')
    parser.add_argument('-c', dest='CrabConfig', type=str, metavar='FILE', nargs='+',
                        help='CRAB3 configuration files (including .py extension)')
    parser.add_argument('--debug', action='store_true', help='More verbose output', dest='debug')
    options = parser.parse_args()
    if options.CrabConfig is None and options.SAMPLE_ID is None:
        parser.error('You must specify some crab tasks or SAMADhi IDs you wish to merge')
    if options.CrabConfig is not None and options.SAMPLE_ID is not None:
        parser.error('You must choose in between merging crab tasks or SAMADhi IDs')
    if (options.CrabConfig is not None and len(options.CrabConfig) < 2) or (options.SAMPLE_ID is not None and len(options.SAMPLE_ID) < 2):
        parser.error('You must have at least 2 samples to merge')
    return options

def load_file(filename):
    directory, module_name = os.path.split(filename)
    module_name = os.path.splitext(module_name)[0]
    path = list(sys.path)
    sys.path.insert(0, directory)
    try:
        module = __import__(module_name)
    finally:
        sys.path[:] = path # restore
    return module

def get_dataset(inputDataset = None, inputID = None):
    dbstore = DbStore()
    if inputDataset is not None:
        resultset = dbstore.find(Dataset, Dataset.name == inputDataset)
    elif inputID is not None:
        resultset = dbstore.find(Dataset, Dataset.dataset_id == inputID)
    return list(resultset.values(Dataset.name, Dataset.dataset_id, Dataset.nevents, Dataset.process))

def get_sample(inputSample = None, inputID = None):
    dbstore = DbStore()
    if inputSample is not None:
        resultset = dbstore.find(Sample, Sample.name == inputSample)
    elif inputID is not None:
        resultset = dbstore.find(Sample, Sample.sample_id == inputID)
    return list(resultset.values(Sample.name, Sample.sample_id, Sample.source_dataset_id, Sample.code_version))

def getGitTagRepoUrl(gitCallPath):
    # get the stuff needed to write a valid url: name on github, name of repo, for both origin and upstream
    proc = subprocess.Popen(['git', 'remote', 'show', 'origin'], cwd = gitCallPath, stdout=subprocess.PIPE)
    remoteOrigin = proc.stdout.read()
    remoteOrigin = [x.split(':')[-1].split('/') for x in remoteOrigin.split('\n') if 'Fetch URL' in x]
    remoteOrigin, repoOrigin = remoteOrigin[0]
    repoOrigin = repoOrigin.strip('.git')
    proc = subprocess.Popen(['git', 'remote', 'show', 'upstream'], cwd = gitCallPath, stdout=subprocess.PIPE)
    remoteUpstream = proc.stdout.read()
    remoteUpstream = [x.split(':')[-1].split('/') for x in remoteUpstream.split('\n') if 'Fetch URL' in x]
    remoteUpstream, repoUpstream = remoteUpstream[0]
    repoUpstream = repoUpstream.strip('.git')
    # get the hash of the commit
    # Well, note that actually it should be the tag if a tag exist, the hash is the fallback solution
    proc = subprocess.Popen(['git', 'describe', '--tags', '--always', '--dirty'], cwd = gitCallPath, stdout=subprocess.PIPE)
    gitHash = proc.stdout.read().strip('\n')
    if( 'dirty' in gitHash ):
        raise AssertionError("Aborting: your working tree for repository", repoOrigin, "is dirty, please clean the changes not staged/committed before inserting this in the database") 
    # get the list of branches in which you can find the hash
    proc = subprocess.Popen(['git', 'branch', '-r', '--contains', gitHash], cwd = gitCallPath, stdout=subprocess.PIPE)
    branch = proc.stdout.read()
    if( 'upstream' in branch ):
        url = "https://github.com/" + remoteUpstream + "/" + repoUpstream + "/tree/" + gitHash
        repo = repoUpstream
    elif( 'origin' in branch ):
        url = "https://github.com/" + remoteOrigin + "/" + repoOrigin + "/tree/" + gitHash
        repo = repoOrigin
    elif( '/' in branch ):
        url = "https://github.com/" + branch.strip(" ").split("/")[0] + "/" + repoOrigin + "/tree/" + branch.strip(" ").split("/")[1]
        repo = repoOrigin
    else:
        print "PLEASE PUSH YOUR CODE!!! this result CANNOT be reproduced / bookkept outside of your ingrid session, so there is no point into putting it in the database, ABORTING now"
        raise AssertionError("Code from repository " + repoUpstream + " has not been pushed")
    return gitHash, repo, url

def add_merged_sample(NAME, type, AnaUrl, FWUrl, samples):
    # samples is a simple dict containing three keys: 'process', 'dataset_id', 'sample_id'
    dbstore = DbStore()
    sample = None

    # check that source dataset exist
    # Skip: should exist, the check has been done before calling this function

    # check that there is no existing entry
    update = False
    localpath = ''
    nevents = 0
    checkExisting = dbstore.find(Sample, Sample.name == unicode(NAME))
    if checkExisting.is_empty():
        sample = Sample(unicode(NAME), unicode(localpath), unicode(type), nevents)
    else:
        update = True
        sample = checkExisting.one()
        sample.removeFiles(dbstore)

    # collecting contents
    sample.nevents_processed = 0
    sample.nevents = 0
    sample.normalization = 1
    sample.event_weight_sum = 0
    extras_event_weight_sum = {}
    dataset_nevents = 0
    processed_lumi = LumiList()
    for i, s in enumerate(samples):
        if i == 0:
            sample.source_dataset_id = s['dataset_id']
            sample.source_sample_id = s['sample_id']
        results = dbstore.find(Sample, Sample.sample_id == s['sample_id'])
        # Should exist, the check has been done before calling this function
        sample.nevents_processed += results[0].nevents_processed
        sample.nevents += results[0].nevents
        sample.event_weight_sum += results[0].event_weight_sum
        extra_sumw = results[0].extras_event_weight_sum
        if extra_sumw is not None:
            extra_sumw = json.loads(extra_sumw)
            for key in extra_sumw:
                try:
                    extras_event_weight_sum[key] += extra_sumw[key]
                except KeyError:
                    extras_event_weight_sum[key] = extra_sumw[key]
        tmp_processed_lumi = results[0].processed_lumi
        if tmp_processed_lumi is not None:
            tmp_processed_lumi = json.loads( tmp_processed_lumi )
            processed_lumi = processed_lumi | LumiList(compactList = tmp_processed_lumi)
        # Get info from file table
        results = dbstore.find(File, File.sample_id == s['sample_id'])
        for lfn, pfn, event_weight_sum, file_extras_event_weight_sum, nevents in list(results.values(File.lfn, File.pfn, File.event_weight_sum, File.extras_event_weight_sum, File.nevents)):
            f = File(lfn, pfn, event_weight_sum, file_extras_event_weight_sum, nevents)
            sample.files.add(f)
        # Get info from parent datasets
        results = dbstore.find(Dataset, Dataset.dataset_id == s['dataset_id'])
        dataset_nevents +=  results[0].nevents
    if len(extras_event_weight_sum) > 0:
        sample.extras_event_weight_sum = unicode(extras_event_weight_sum)
    if len(processed_lumi.getCompactList()) > 0:
        sample.processed_lumi = unicode(json.dumps(processed_lumi.getCompactList()))
    sample.code_version = unicode(AnaUrl + ' ' + FWUrl) #NB: limited to 255 characters, but so far so good
    if sample.nevents_processed != dataset_nevents:
        sample.user_comment = unicode("Sample was not fully processed, only " + str(sample.nevents_processed) + "/" + str(dataset_nevents) + " events were processed")
    else:
        sample.user_comment = u""
    sample.author = unicode(getpwuid(os.stat(os.getcwd()).st_uid).pw_name)

    if not update:
        dbstore.add(sample)
        if sample.luminosity is None:
            sample.luminosity = sample.getLuminosity()

        print sample

        dbstore.commit()
        return

    else:
        sample.luminosity = sample.getLuminosity()
        print("Sample updated")
        print(sample)

        dbstore.commit()
        return

    # rollback
    dbstore.rollback()

def main():
    options = get_options()

    import platform
    if 'ingrid' in platform.node():
        storagePrefix = "/storage/data/cms"
    else:
        storagePrefix = "root://cms-xrd-global.cern.ch/"

    print "##### Running on several tasks: will (attempt to) merge them in a single sample"
    print("")

    print "##### Figure out the code(s) version"
    FWHash = ''
    FWRepo = ''
    FWUrl = ''
    AnaHash = ''
    AnaRepo = ''
    AnaUrl = ''
    if options.CrabConfig is not None and len(options.CrabConfig) > 1:
        module0 = load_file(options.CrabConfig[0])
        psetName0 = module0.config.JobType.psetName
        # first the version of the framework
        FWHash, FWRepo, FWUrl = getGitTagRepoUrl( os.path.join(CMSSW_BASE, 'src/cp3_llbb/Framework') )
        # then the version of the analyzer
        AnaHash, AnaRepo, AnaUrl = getGitTagRepoUrl( os.path.dirname( psetName0 ) )
    elif options.SAMPLE_ID is not None and len(options.SAMPLE_ID) > 1:
        sample = get_sample(inputID = options.SAMPLE_ID[0])
        if len(sample) == 0:
            raise AssertionError("Aborting: the sample", NAME, "does not exist in the database, please insert it first") 
        sample_name, sample_id, dataset_id, code_version = sample[0]
        AnaUrl, FWUrl = code_version.split()
        FWRepo = FWUrl.split('tree')[0].split('/')[-2]
        FWHash = FWUrl.split('tree')[1].strip('/')
        AnaRepo = AnaUrl.split('tree')[0].split('/')[-2]
        AnaHash = AnaUrl.split('tree')[1].strip('/')
    print "FWUrl=", FWUrl
    print "AnaUrl=", AnaUrl
    print("")
        
        
    
    print "##### Check the samples already exist in the database"
    samples = []
    if options.CrabConfig is not None and len(options.CrabConfig) > 1:
        for CrabConfig in options.CrabConfig:
            module = load_file(CrabConfig)
            requestName = module.config.General.requestName
            NAME = requestName + '_' + FWHash + '_' + AnaRepo + '_' + AnaHash
            sample = get_sample(inputSample = unicode(NAME))
            if len(sample) == 0:
                raise AssertionError("Aborting: the sample", NAME, "does not exist in the database, please insert it first") 
            sample_name, sample_id, dataset_id, code_version = sample[0]
            dataset = get_dataset(inputID = dataset_id)
            dataset_name, dataset_id, dataset_nevents, dataset_process = dataset[0]
            samples.append({'sample_id': sample_id, 'process':dataset_process, 'dataset_id':dataset_id})
    elif options.SAMPLE_ID is not None and len(options.SAMPLE_ID) > 1:
        for SAMPLE_ID in options.SAMPLE_ID:
            sample = get_sample(inputID = SAMPLE_ID)
            if len(sample) == 0:
                raise AssertionError("Aborting: the sample", NAME, "does not exist in the database, please insert it first") 
            sample_name, sample_id, dataset_id, code_version = sample[0]
            dataset = get_dataset(inputID = dataset_id)
            dataset_name, dataset_id, dataset_nevents, dataset_process = dataset[0]
            samples.append({'sample_id': sample_id, 'process':dataset_process, 'dataset_id':dataset_id})
            
    if len( set([ x['process'] for x in samples ]) ) != 1:
        print "samples=", samples
        raise AssertionError("Aborting: not all samples are from the same process, I am not sure what you are trying to do... but doing it in an automated way is probably asking for trouble")
    elif len( set([ x['sample_id'] for x in samples ]) ) < len(samples):
        print "samples=", samples
        raise AssertionError('Aborting: you are trying to merge a sample with itself')
    elif len( set([ x['dataset_id'] for x in samples ]) ) < len(samples):
        print "samples=", samples
        raise AssertionError('Aborting: you are trying to merge samples inheriting from the same dataset')
    else:
        print "done: all samples exist and inherit from the same process"
    print("")

    print "##### Constructing the merged sample"
    # Note that not all info make sense since this is an artificial merging (e.g.: path)
    # So in case of ill-defined quantity, we take the one from the first sample in the list
    NAME = ''
    for i, s in enumerate(samples):
        if i == 0:
            NAME += s['process']
            NAME += '_extended_' + str(s['sample_id'])
        else:
            NAME += '_plus_' + str(s['sample_id'])
    NAME += '_' + FWHash + '_' + AnaRepo + '_' + AnaHash 
    add_merged_sample(NAME, 'NTUPLES', AnaUrl, FWUrl, samples)

   
if __name__ == '__main__':
    main() 
