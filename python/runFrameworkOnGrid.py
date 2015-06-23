#! /usr/bin/env python

__author__ = 'sbrochet'

"""
This script will be executed on a grid cluster by CRAB.
It extracts the list of files to run over, as well as the number of events, and the run / lumi list
"""

def decode_lfn(lfn):
    """
    Convert LFN to PFN. To do that, call ``edmFileUtil``
    :param lfn:
    :return:
    """
    import subprocess
    arg = ['edmFileUtil', '-d', lfn]
    return subprocess.check_output(arg).strip().replace('\n', '')

def test_root_open(f):
    """
    Test if ROOT can open file ``f``
    :param f:
    :return:
    """
    import ROOT

    oldlevel = ROOT.gErrorIgnoreLevel
    ROOT.gErrorIgnoreLevel = ROOT.kFatal
    ret = bool(ROOT.TFile.Open(f))
    ROOT.gErrorIgnoreLevel = oldlevel

    print('Checking if the file %r is readable: %s' % (f, 'yes' if ret else 'no'))

    return ret

import argparse
parser = argparse.ArgumentParser(description='Execute the framework on a remote cluster')
parser.add_argument('job_number', metavar='N', type=int, help='The current job number')
parser.add_argument('configuration', type=str, help='Analysis configuration file')

args = parser.parse_args()

import PSet

configuration_file = args.configuration.split('=')[1]
files = list(PSet.process.source.fileNames)
lumi_mask = PSet.process.source.lumisToProcess if hasattr(PSet.process.source, 'lumisToProcess') else None
n_events = PSet.process.maxEvents.input

absolute_files = []
for file in files:
    if file.startswith('/store'):

        # Check if the file is locally accessible
        pfn = decode_lfn(file)
        if test_root_open(pfn):
            absolute_files.append(pfn)
        else:
            absolute_files.append('root://xrootd-cms.infn.it/%s' % file)

    else:
        absolute_files.append(file)

print('')

# Dump variable to stdout for debugging purpose:
import pprint
print('Input files:')
pprint.pprint(absolute_files)

print('Number of events: %s' % str(n_events))

if lumi_mask is not None:
    print('Lumi mask: %s' % str(lumi_mask))

print('')
print('Running framework.')
print('')

# Include framework
import Framework

# Let's go!
Framework.run(configuration_file, absolute_files, 'output.root', n_events, 'FrameworkJobReport.xml',
              lumi_mask=lumi_mask)
