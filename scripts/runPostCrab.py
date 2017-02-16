#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division

import argparse
import os
import sys
import json
from pwd import getpwuid

# import SAMADhi stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE, 'bin', SCRAM_ARCH))

# Add default ingrid storm package
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/storm-0.20-py2.7-linux-x86_64.egg')
sys.path.append('/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/lib/python2.7/site-packages/MySQL_python-1.2.3-py2.7-linux-x86_64.egg')

from SAMADhi import Dataset, Sample, File, DbStore
import das_import

# import a bit of ROOT
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gROOT.Reset()

from cp3_llbb.GridIn import utils

def get_file_data(pfn):
    """
    Return the sum of event weights and the entries of the framework output
    """
    if not os.path.isfile(pfn):
        raise IOError('The output file %r is missing on the disk. You need to relaunch the associated job.' % pfn)

    f = ROOT.TFile.Open(pfn)
    if not f:
        raise IOError('The output file %r is missing on the disk or is corrupted. You need to relaunch the associated job.' % pfn)

    nominal_sumw = f.Get("event_weight_sum")
    if nominal_sumw:
        nominal_sumw = nominal_sumw.GetVal()
    else:
        raise IOError('Output file %r is corrupted. "event_weight_sum" is missing.' % pfn)

    # Grab extras sum of event weight
    extras_sumw = {}
    for key in f.GetListOfKeys():
        if key.GetName().startswith('event_weight_sum_'):
            suffix = key.GetName().replace('event_weight_sum_', '')
            obj = key.ReadObj()
            if obj:
                extras_sumw[suffix] = obj.GetVal()

    entries = None
    tree = f.Get("t")
    if tree:
        entries = tree.GetEntriesFast()
    else:
        raise IOError('Output file %r is corrupted. Tree is missing.' % pfn)

    return (nominal_sumw, extras_sumw, entries)

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Gather the information on a processed sample and insert the information in SAMADhi')
    parser.add_argument('CrabFolder', type=str, nargs='+', metavar='DIR',
                        help='Crab task folder')
    parser.add_argument('--debug', action='store_true', help='More verbose output', dest='debug')
    options = parser.parse_args()
    return options

def get_dataset(inputDataset):
    dbstore = DbStore()
    resultset = dbstore.find(Dataset, Dataset.name==inputDataset)
    return list(resultset.values(Dataset.name, Dataset.dataset_id, Dataset.nevents))


def add_sample(NAME, localpath, type, nevents, nselected, AnaUrl, FWUrl, dataset_id, sumw, extras_sumw, has_job_processed_everything, dataset_nevents, files, processed_lumi=None):
    dbstore = DbStore()

    sample = None

    # check that source dataset exist
    if dbstore.find(Dataset, Dataset.dataset_id == dataset_id).is_empty():
        raise IndexError("No dataset with such index: %d" % sample.dataset_id)

    # check that there is no existing entry
    update = False
    checkExisting = dbstore.find(Sample, Sample.name == unicode(NAME))
    if checkExisting.is_empty():
        sample = Sample(unicode(NAME), unicode(localpath), unicode(type), nevents)
    else:
        update = True
        sample = checkExisting.one()
        sample.removeFiles(dbstore)

    sample.nevents_processed = nevents
    sample.nevents = nselected
    sample.normalization = 1
    sample.event_weight_sum = sumw
    sample.extras_event_weight_sum = unicode(json.dumps(extras_sumw, separators=(',', ':')))
    sample.code_version = unicode(AnaUrl + ' ' + FWUrl) #NB: limited to 255 characters, but so far so good
    if not has_job_processed_everything:
        sample.user_comment = unicode("Sample was not fully processed, only " + str(nevents) + "/" + str(dataset_nevents) + " events were processed")
    else:
        sample.user_comment = u""
    sample.source_dataset_id = dataset_id
    sample.author = unicode(getpwuid(os.stat(os.getcwd()).st_uid).pw_name)

    if processed_lumi:
        # Convert to json
        processed_lumi = json.dumps(processed_lumi, separators=(',', ':'))
        sample.processed_lumi = unicode(processed_lumi)
    else:
        sample.processed_lumi = None

    for f in files:
        sample.files.add(f)

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

    for taskdir in options.CrabFolder:
        print "##### Get crab task information for task folder '{}'".format(taskdir)

        crab_request = utils.load_request(taskdir)
        config = crab_request['OriginalConfig']

        workArea = config.General.workArea
        requestName = config.General.requestName
        psetName = config.JobType.psetName
        inputDataset = unicode(config.Data.inputDataset)
        print "done"

        print("")
        
        print "##### Check if the dataset exists in the database"
        # if yes then grab its ID
        # if not then run das_import.py to add it
        # print inputDataset
        values = get_dataset(inputDataset)
        # print values
        if( len(values) == 0 ):
            print "Importing CMS dataset"
            das_import.import_cms_dataset(inputDataset)
            print "done"
            values = get_dataset(inputDataset)
        # if there is more than one sample then we're in trouble, crash here
        assert( len(values) == 1 )
        dataset_name, dataset_id, dataset_nevents = values[0]
        print "done"

        print("")
        
        print "##### Get info from crab (outputs, report)"
        # Since the API outputs AND prints the same data, hide whatever is printed on screen
        saved_stdout, saved_stderr = sys.stdout, sys.stderr
        if not options.debug:
            sys.stdout = open(os.devnull, "w")

        # list output
        output_files = utils.send_crab_command('getoutput', '--dump', dir = taskdir )
        # get crab report
        report = utils.send_crab_command('report', dir = taskdir )
        # restore print to stdout 
        if not options.debug:
            sys.stdout = saved_stdout
    #    print "log_files=", log_files
    #    print "output_files=", output_files
    #    print "report=", report
        print "done"

        print("")

        print "##### Get information from the output files (%d files)" % (len(output_files['lfn']))
        files = []
        for (i, lfn) in enumerate(output_files['lfn']):
            pfn = output_files['pfn'][i]
            files.append({'lfn': lfn, 'pfn': pfn})

        # DEBUG
        #files.append({'lfn': '/store/user/sbrochet/TTTo2L2Nu_13TeV-powheg/TTTo2L2Nu_13TeV-powheg_MiniAODv2/160210_181039/0000/output_mc_1.root', 'pfn': 'srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/user/sbrochet/TTTo2L2Nu_13TeV-powheg/TTTo2L2Nu_13TeV-powheg_MiniAODv2/160210_181039/0000/output_mc_1.root'})

        folder = os.path.dirname(output_files['lfn'][0])
        folder = storagePrefix + folder

        db_files = []
        dataset_sumw = 0
        dataset_extras_sumw = {}
        dataset_nselected = 0
        file_missing = False
        utils.print_progress(0, len(files), prefix='Progress:')
        for i, f in enumerate(files):
            (sumw, extras_sumw, entries) = get_file_data(storagePrefix + f['lfn'])
            utils.print_progress(i + 1, len(files), prefix='Progress:')

            dataset_sumw += sumw
            dataset_extras_sumw = utils.sum_dicts(dataset_extras_sumw, extras_sumw)
            dataset_nselected += entries

            # Convert python dict to json
            extras_sumw_json = unicode(json.dumps(extras_sumw, separators=(',', ':')))

            db_files.append(File(unicode(f['lfn']), unicode(f['pfn']), sumw, extras_sumw_json, entries))

        print "∑w = %.4f" % dataset_sumw
        print "Number of selected events: %d" % dataset_nselected
        print "Number of output files (crab / really on the storage): %d / %d" % (len(files), len(db_files))

        print("")

        print "##### Check if the job processed the whole sample"
        if 'numEventsRead' not in report:
            print("Warning: crab report is incomplete, it's not possible to check if the job processed everything.")
            has_job_processed_everything = True
        else:
            has_job_processed_everything = (dataset_nevents == report['numEventsRead']) and not file_missing

        is_data = (config.Data.splitting == 'LumiBased')
        if has_job_processed_everything:
            print "done"
        else:
            if is_data:
                # This is data, it is expected to not run on everything given we use a lumiMask
                print "done"
            else:
                # Warn
                print "Warning: You are about to add in the DB a sample which has not been completely processed (%d events out of %d, %.2f%%)" % (report['numEventsRead'], dataset_nevents, report['numEventsRead'] / dataset_nevents * 100)
                print "If you want to update this sample later on with more statistics, simply re-execute this script with the same arguments."

        print("")

        processed_lumi = None
        if is_data:
            processed_lumi = report['processedLumis']

        print "##### Figure out the code(s) version"
        # first the version of the framework
        FWHash, FWRepo, FWUrl = utils.getGitTagRepoUrl( os.path.join(CMSSW_BASE, 'src/cp3_llbb/Framework') )
        print "FWUrl=", FWUrl
        # then the version of the analyzer
        AnaHash, AnaRepo, AnaUrl = utils.getGitTagRepoUrl( os.path.dirname( psetName ) )
        print "AnaUrl=", AnaUrl

        print("")

        print "##### Put it all together: write this sample into the database"
        # all the info we have gathered is:
        # workArea
        # requestName
        # psetName
        # inputDataset
        # dataset_id
        # report['numEventsRead']) (not necessarily equal to dataset_nevents)
        # log_files
        # output_files
        # report
        # FWHash, FWRepo, FWUrl
        # AnaHash, AnaRepo, AnaUrl
        # dataset_nselected
        # localpath
        NAME = requestName + '_' + FWHash + '_' + AnaRepo + '_' + AnaHash
        add_sample(NAME, folder, "NTUPLES", dataset_nevents, dataset_nselected, AnaUrl, FWUrl, dataset_id, dataset_sumw, dataset_extras_sumw, has_job_processed_everything, dataset_nevents, db_files, processed_lumi)

if __name__ == '__main__':
    main() 
