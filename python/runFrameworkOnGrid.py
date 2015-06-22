#! /usr/bin/env python

__author__ = 'sbrochet'

"""
This script will be executed on a grid cluster by CRAB.
It extracts the list of files to run over, as well as the number of events, and the run / lumi list
"""

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
        absolute_files.append('root://xrootd-cms.infn.it/%s' % file)
    else:
        absolute_files.append(file)

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
