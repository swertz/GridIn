#! /usr/bin/env python

__author__ = 'sbrochet'

"""
Launch crab or condor and run the framework on multiple datasets
"""

from CRABAPI.RawCommand import crabCommand

import json
import copy
import os
import argparse
import sys
import subprocess

from multiprocessing import Pool, Lock
lock = Lock()

from cp3_llbb.GridIn.default_crab_config import create_config

CMSSW_ROOT = os.path.join(os.environ['CMSSW_BASE'], 'src')
GRIDIN_ROOT = os.path.join(os.environ['CMSSW_BASE'], 'src/cp3_llbb/GridIn')
DATASETS_ROOT = os.path.join(os.environ['CMSSW_BASE'], 'src/cp3_llbb/Datasets')

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Launch crab over multiple datasets.')

    parser.add_argument('--submit', action='store_true', dest='submit',
                        help='Submit all the tasks to the CRAB server')

    parser.add_argument('-j', '--cores', type=int, action='store', dest='processes', metavar='N', default='4',
                        help='Number of core to use during the crab tasks creation')

    parser.add_argument('-l', '--lumi-mask', type=str, required=False, dest='lumi_mask', metavar='URL',
                        help='URL to the luminosity mask to use when running on data')

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument('--only-mc', action='store_true', dest='only_mc', help='Only run over MC datasets',)
    group.add_argument('--only-data', action='store_true', dest='only_data', help='Only run over data datasets')

    parser.add_argument('-f', '--filter', type=str, required=False, dest='filter', metavar='FILTER',
                        help='If specified, only keep sample groups matching this filter. Glob syntax is supported')

    parser.add_argument('analyses', type=str, nargs='+', metavar='FILE',
                        help='List of JSON file describing the analysis.')

    options = parser.parse_args()

    if options.analyses is None:
        parser.error('You must specify at least one file describing an analysis.')

    return options

def findPSet(pset):
    c = pset
    if not os.path.isfile(c):
        # Try to find the psetName file
        filename = os.path.basename(c)
        path = os.path.join(CMSSW_ROOT, 'cp3_llbb')
        c = None
        for root, dirs, files in os.walk(path):
            if filename in files:
                c = os.path.join(root, filename)
                break

        if c is None:
            raise IOError('Configuration file %r not found inside the cp3_llbb package' % filename)

    return c

psets = {}
def loadPSet(pset, onMC):
    global psets

    lock.acquire()

    key = (onMC, pset)

    if key in psets:
        lock.release()
        return psets[key]

    directory, module_name = os.path.split(pset)
    module_name = os.path.splitext(module_name)[0]
    old_path = list(sys.path)
    sys.path.insert(0, directory)

    # Clean argv for CMSSW argument parser
    old_argv = list(sys.argv)
    sys.argv[1:] = ['runOnData=%d' % (0 if onMC else 1)]

    print("Loading CMSSW configuration %s..." % module_name)
    old_stdout = sys.stdout
    try:
        # Redirect stdout to /dev/null
        with open(os.devnull, 'w') as f:
            sys.stdout = f
            module = __import__(module_name, {}, {}, [])

        psets[key] = module
        del sys.modules[module_name]
        return module
    finally:
        sys.stdout = old_stdout
        print("Done.")
        lock.release()
        sys.path[:] = old_path # restore
        sys.argv[:] = old_argv

def submit(job):
    module = loadPSet(job['pset'], job['on_mc'])

    c = copy.deepcopy(job['crab_config'])

    c.JobType.psetName = job['pset']
    c.JobType.outputFiles.append(module.process.framework.output.value())

    if hasattr(module.process, 'gridin') and hasattr(module.process.gridin, 'input_files') and len(module.process.gridin.input_files) > 0:
        if not hasattr(c.JobType, 'inputFiles'):
            c.JobType.inputFiles = []

        c.JobType.inputFiles += module.process.gridin.input_files

    c.General.requestName = job['metadata']['name']
    c.Data.outputDatasetTag = job['metadata']['name']

    c.Data.inputDataset = job['dataset']

    try:
        splittingType, splittingValueStr = job['splitting'].split(":")
        if splittingType == "relative":
            c.Data.unitsPerJob = int(round(float(splittingValueStr) * job['metadata']['units_per_job']))
        elif splittingType == "absolute":
            c.Data.unitsPerJob = int(splittingValueStr)
        else:
            raise Exception("Invalid splitting setting '{0}', should take the form of 'relative:float' or 'absolute:int'".format(options.splitting))
    except:
        raise Exception("Cannot parse splitting setting '{0}', should take the form of 'relative:float' or 'absolute:int'".format(options.splitting))

    pyCfgParams = [str('runOnData=%d' % (0 if job['on_mc'] else 1))]

    era = job['metadata']['era']
    assert era == '25ns' or era == '50ns'
    pyCfgParams += [str('era=%s' % era)]

    if 'globalTag' in job['metadata']:
        pyCfgParams += [str('globalTag=%s' % job['metadata']['globalTag'])]

    # Fix process name for PromptReco, which is RECO instead of PAT
    if not job['on_mc'] and 'PromptReco' in dataset:
        pyCfgParams += [str('process=RECO')]

    # Fix hlt process name for some (signal) 80X samples, which is HLT2 instead of HLT
    if job['on_mc'] and 'reHLT_80X' in dataset:
        pyCfgParams += [str('hltProcessName=HLT2')]

    c.JobType.pyCfgParams = pyCfgParams

    print("Submitting new task %r" % c.General.requestName)
    print("\tDataset: %s" % dataset)

    if not job['on_mc']:
        c.Data.runRange = '%d-%d' % (job['metadata']['run_range'][0], job['metadata']['run_range'][1])
        if not 'certified_lumi_file' in job['metadata'] and not options.lumi_mask:
            raise Exception('You are running on data but no luminosity mask is specified for task %r. Please add the \'--lumi-mask\' argument or use the \'certified_lumi_file\' key inside the JSON file' % (c.General.requestName))

        c.Data.lumiMask = options.lumi_mask if options.lumi_mask else job['metadata']['certified_lumi_file']

    # Create output file in case something goes wrong with submit
    crab_config_file = 'crab_' + job['analysis'] + '_' + c.General.requestName + '.py'
    with open(crab_config_file, 'w') as f:
        f.write(str(c))

    if options.submit:
        subprocess.call(['crab', 'submit', crab_config_file])
    else:
        print('Configuration file saved as %r' % (crab_config_file))

options = get_options()

analyses = {}
for j in options.analyses:
    with open(j) as f:
        data = json.load(f)
        analyses[data["name"]] = data

# Load all known datasets
import glob
datasets = {}
for dataset in glob.glob(os.path.join(DATASETS_ROOT, "datasets", "*.json")):
    with open(dataset) as f:
        datasets.update(json.load(f))

crab_config_cache = {}
for name, analysis in analyses.items():

    def globMatch(value, pattern):
        import fnmatch
        return fnmatch.fnmatch(value, pattern)

    def globIn(value, patterns):
        """
        Test if 'value' is matched by any pattern in 'patterns'
        """
        for pattern in patterns:
            if globMatch(value, pattern):
                return True

        return False

    if not 'splitting' in analysis:
        analysis['splitting'] = 'relative:1'

    # Filter groups if requested
    if options.filter:
        def filterGroups(groups):
            result = []
            for group in groups:
                if globMatch(group, options.filter):
                    result.append(group)

            return result

        analysis['samples']['mc'] = filterGroups(analysis['samples']['mc'])
        analysis['samples']['data'] = filterGroups(analysis['samples']['data'])

    # Create jobs
    jobs = []

    matched_group = []

    for group, group_samples in datasets.items():
        data = globIn(group, analysis["samples"]["data"])
        mc = globIn(group, analysis["samples"]["mc"])

        if not data and not mc:
            continue

        if options.only_mc and not mc:
            continue

        if options.only_data and not data:
            continue

        matched_group.append(group)

        if mc in crab_config_cache:
            c = crab_config_cache[mc]
        else:
            c = create_config(mc)
            crab_config_cache[mc] = c

        pset = findPSet(analysis['configuration'].replace('%TYPE%', 'MC' if mc else 'Data'))
        loadPSet(pset, mc)

        for dataset, metadata in group_samples.items():

            job = {
                    'analysis': name,
                    'splitting': analysis['splitting'],
                    'on_mc': mc,
                    'pset': pset,
                    'dataset': dataset,
                    'metadata': metadata,
                    'crab_config': c
                    }

            jobs.append(job)

    def ensureGroup(type):
        for sample in analysis["samples"][type]:
            found = False
            for group in matched_group:
                if globMatch(group, sample):
                    found = True
            if not found:
                raise Exception("Sample group %r requested for %s in analysis %r not found in the list of datasets." % (str(sample), type, str(name)))

    if not options.only_mc:
        ensureGroup("data")

    if not options.only_data:
        ensureGroup("mc")

pool = Pool(processes=options.processes)
pool.map(submit, jobs)

