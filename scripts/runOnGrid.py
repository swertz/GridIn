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

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Launch crab over multiple datasets.')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--mc', action='store_true', dest='mc', help='Run over MC datasets',)
    group.add_argument('--data', action='store_true', dest='data', help='Run over data datasets')

    parser.add_argument('-c', '--configuration', type=str, required=True, dest='psetName', metavar='FILE',
                        help='Analysis configuration file (including .py extension).')

    parser.add_argument('--submit', action='store_true', dest='submit',
                        help='Submit all the tasks to the CRAB server')

    parser.add_argument('-j', '--cores', type=int, action='store', dest='processes', metavar='N', default='4',
                        help='Number of core to use during the crab tasks creation')

    parser.add_argument("--splitting-factor", type=str, required=False, dest="splitting", metavar="SPLITTING", default="relative:1",
                        help="Splitting factor (either 'relative:float', to be multiplied with the value in the sample json, or 'absolute:int' to set it explicitly)")

    parser.add_argument('-l', '--lumi-mask', type=str, required=False, dest='lumi_mask', metavar='URL',
                        help='URL to the luminosity mask to use when running on data')

    parser.add_argument('datasets', type=str, nargs='+', metavar='FILE',
                        help='JSON files listings datasets to run over.')

    options = parser.parse_args()

    if options.datasets is None:
        parser.error('You must specify a file listings the datasets to run over.')

    c = options.psetName
    if not os.path.isfile(c):
        # Try to find the psetName file
        filename = os.path.basename(c)
        path = os.path.join(os.environ['CMSSW_BASE'], 'src/cp3_llbb')
        c = None
        for root, dirs, files in os.walk(path):
            if filename in files:
                c = os.path.join(root, filename)
                break

        if c is None:
            raise IOError('Configuration file %r not found inside the cp3_llbb package' % filename)

    options.psetName = c

    return options

options = get_options()

# get the name of the output file
filename = options.psetName
directory, module_name = os.path.split(filename)
module_name = os.path.splitext(module_name)[0]
path = list(sys.path)
sys.path.insert(0, directory)
try:
  module = __import__(module_name)
finally:
  sys.path[:] = path # restore

print("")

options.outputFile = module.process.framework.output.value()

datasets = {}
for dataset_file in options.datasets:
    with open(dataset_file) as f:
        datasets.update(json.load(f))

from cp3_llbb.GridIn.default_crab_config import create_config

config = create_config(options.mc)

def submit(dataset, opt):
    c = copy.deepcopy(config)

    c.JobType.psetName = options.psetName
    c.JobType.outputFiles.append(options.outputFile)

    if hasattr(module.process, 'gridin') and hasattr(module.process.gridin, 'input_files') and len(module.process.gridin.input_files) > 0:
        if not hasattr(c.JobType, 'inputFiles'):
            c.JobType.inputFiles = []

        c.JobType.inputFiles += module.process.gridin.input_files

    c.General.requestName = opt['name']
    c.Data.outputDatasetTag = opt['name']

    c.Data.inputDataset = dataset

    try:
        splittingType, splittingValueStr = options.splitting.split(":")
        if splittingType == "relative":
            c.Data.unitsPerJob = int(round(float(splittingValueStr)*opt['units_per_job']))
        elif splittingType == "absolute":
            c.Data.unitsPerJob = int(splittingValueStr)
        else:
            raise Exception("Invalid splitting setting '{0}', should take the form of 'relative:float' or 'absolute:int'".format(options.splitting))
    except:
        raise Exception("Cannot parse splitting setting '{0}', should take the form of 'relative:float' or 'absolute:int'".format(options.splitting))

    pyCfgParams = []

    era = opt['era']
    assert era == '25ns' or era == '50ns'
    pyCfgParams += [str('era=%s' % era)]

    if 'globalTag' in opt:
        pyCfgParams += [str('globalTag=%s' % opt['globalTag'])]

    # Fix process name for PromptReco, which is RECO instead of PAT
    if options.data and 'PromptReco' in dataset:
        pyCfgParams += [str('process=RECO')]

    c.JobType.pyCfgParams = pyCfgParams

    print("Submitting new task %r" % opt['name'])
    print("\tDataset: %s" % dataset)

    if options.data:
        c.Data.runRange = '%d-%d' % (opt['run_range'][0], opt['run_range'][1])
        if not 'certified_lumi_file' in opt and not options.lumi_mask:
            raise Exception('You are running on data but no luminosity mask is specified for task %r. Please add the \'--lumi-mask\' argument or use the \'certified_lumi_file\' key inside the JSON file' % (opt['name']))

        c.Data.lumiMask = options.lumi_mask if options.lumi_mask else opt['certified_lumi_file']

    # Create output file in case something goes wrong with submit
    crab_config_file = 'crab_' + opt['name'] + '.py'
    with open(crab_config_file, 'w') as f:
        f.write(str(c))

    if options.submit:
        subprocess.call(['crab', 'submit', crab_config_file])
    else:
        print('Configuration file saved as %r' % ('crab_' + opt['name'] + '.py'))

def submit_wrapper(args):
    submit(*args)

from multiprocessing import Pool
pool = Pool(processes=options.processes)
pool.map(submit_wrapper, datasets.items())

