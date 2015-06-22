__author__ = 'sbrochet'

import FWCore.ParameterSet.Config as cms

process = cms.Process('Dummy')

process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring())
process.options = cms.untracked.PSet(wantSummary=cms.untracked.bool(False))
process.output = cms.OutputModule("PoolOutputModule",
    outputCommands = cms.untracked.vstring("drop *"),
    fileName=cms.untracked.string('output.root'),
)
process.out = cms.EndPath(process.output)
