#!/nfs/soft/python/python-2.7.5-sl6_amd64_gcc44/bin/python

import argparse
import os
import sys
import subprocess
import tarfile
import contextlib
# import SAMADhi stuff
CMSSW_BASE = os.environ['CMSSW_BASE']
SCRAM_ARCH = os.environ['SCRAM_ARCH']
sys.path.append(os.path.join(CMSSW_BASE,'bin', SCRAM_ARCH))
import SAMADhi
import das_import
import add_sample
# import CRAB3 stuff
from CRABAPI.RawCommand import crabCommand

def get_options():
    """
    Parse and return the arguments provided by the user.
    """
    parser = argparse.ArgumentParser(description='Gather the information on a processed sample and insert the information in SAMADhi')
    parser.add_argument('CrabConfig', type=str, metavar='FILE',
                        help='CRAB3 configuration file (including .py extension).')
    options = parser.parse_args()
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

def get_dataset(inputDataset):
    dbstore = SAMADhi.DbStore()
    resultset = dbstore.find(SAMADhi.Dataset, SAMADhi.Dataset.name==inputDataset)
    return list(resultset.values(SAMADhi.Dataset.name, SAMADhi.Dataset.dataset_id, SAMADhi.Dataset.nevents))

def getGitTagBranchUrl(gitCallPath):
    # get the hash of the commit
    # Well, note that actually it should be the tag if a tag exist, the hash is the fallback solution
    proc = subprocess.Popen(['git', 'describe', '--tags', '--always'], cwd = gitCallPath, stdout=subprocess.PIPE)
    gitHash = proc.stdout.read().strip('\n')
    # get the list of branches in which you can find the hash
    proc = subprocess.Popen(['git', 'branch', '-r', '--contains', gitHash], cwd = gitCallPath, stdout=subprocess.PIPE)
    branch = proc.stdout.read()
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
    if( 'upstream' in branch ):
        url = "https://github.com/" + remoteUpstream + "/" + repoUpstream + "/tree/" + gitHash
    elif( 'origin' in branch ):
        url = "https://github.com/" + remoteOrigin + "/" + repoOrigin + "/tree/" + gitHash
        # FIXME: advertize you should consider a pull request?
#        branches = [x.strip().split('/')[-1] for x in branch.strip().split('\n') if 'HEAD' not in x and 'master' not in x and 'origin' in x]
#        print "WARNING: please consider merging your changes to upstream by opening a pull request, for example:"
#        print "\thttps://github.com/cp3-llbb/Framework/compare/master..." + githubUser + ":" + branches[0] + "?expand=1"
    else:
        print "PLEASE PUSH YOUR CODE!!! this result CANNOT be reproduced / bookkept outside of your ingrid session, so there is no point into putting it in the database, ABORTING now"
        raise AssertionError("Code from repository " + repoUpstream + " has not been pushed")
    return gitHash, branch, url

def main():
    options = get_options()
    ingridStoragePrefix = "/storage/data/cms"

    print "##### Get information out of the crab config file (work area, dataset, pset)"
    module = load_file(options.CrabConfig)
    workArea = module.config.General.workArea
    requestName = module.config.General.requestName
    psetName = module.config.JobType.psetName
    inputDataset = unicode(module.config.Data.inputDataset)
    
    print "##### Check the dataset exists in the database"
    # if yes then grab its ID
    # if not then run das_import.py to add it
    values = get_dataset(inputDataset)
    if( len(values) == 0 ):
        tmp_sysargv = sys.argv
        sys.argv = ["das_import.py", inputDataset]
        das_import.main()
        sys.argv = tmp_sysargv
        values = get_dataset(inputDataset)
    # if there is more than one sample then we're in trouble, crash here
    assert( len(values) == 1 )
    dataset_name, dataset_id, dataset_nevents = values[0]
    
    print "##### Get info from crab (logs, outputs, report)"
    taskdir = os.path.join(workArea, 'crab_' + requestName)
    # list logs
    log_files = crabCommand('getlog', '--dump', dir = taskdir )
    # list output
    output_files = crabCommand('getoutput', '--dump', dir = taskdir )
    # get crab report
    report = crabCommand('report', dir = taskdir )
#    print report

    print "##### Check the job processed the whole sample"
    # FIXME: should it rather be just a warning ? Will likely be problematic at some point for big samples
    # FIXME: if changing to a warning, be careful what you actually insert in the DB
    assert( dataset_nevents == report['eventsRead'] )

    print "##### Figure out the code(s) version"
    # first the version of the framework
    FWHash, FWBranch, FWUrl = getGitTagBranchUrl( os.path.join(CMSSW_BASE, 'src/cp3_llbb/Framework') )
    # then the version of the analyzer
    AnaHash, AnaBranch, AnaUrl = getGitTagBranchUrl( os.path.dirname( psetName ) )

    print "##### Figure out the number of selected events"
    # Need to get this from the output log of the jobs, and sum them all
#    log_files = {'lfn': ['/store/user/obondu/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_13TeV-madgraph/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_Asympt25ns/150728_092137/0000/log/cmsRun_1.log.tar.gz'], 'pfn': ['srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/user/obondu/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_13TeV-madgraph/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_Asympt25ns/150728_092137/0000/log/cmsRun_1.log.tar.gz']}
    nselected = 0
    for lfn in log_files['lfn']:
        tarLog =  ingridStoragePrefix + lfn
        with tarfile.open(tarLog) as tar:
            for tarFile in tar.getmembers():
                if 'stdout' not in tarFile.name:
                    continue
                # For some reason, even though we are using python 2.7, the with statement here seems broken... Using contextlib to handle the file opening / reading cleanly
                with contextlib.closing(tar.extractfile(tarFile)) as file:
                    for line in file:
                        if 'processed' not in line and 'selected' not in line:
                            continue
                        l = line.split()
                        nselected  += int(line.split()[3])
#    print nselected

    print "##### For the path, check that the files there do correspond EXACTLY to the list of output files from crab"
    # (crab being crab, we're never too careful!)
#    output_files = {'lfn': ['/store/user/obondu/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_13TeV-madgraph/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_Asympt25ns/150728_092137/0000/output_mc_1.root'], 'pfn': ['srm://ingrid-se02.cism.ucl.ac.be:8444/srm/managerv2?SFN=/storage/data/cms/store/user/obondu/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_13TeV-madgraph/GluGluToRadionToHHTo2B2VTo2L2Nu_M-260_narrow_Asympt25ns/150728_092137/0000/output_mc_1.root']}
    # first check what we do have locally
    p = os.path.dirname( output_files['lfn'][0] )
    localpath = ingridStoragePrefix + p
    localfiles = [ os.path.join(p, f) for f in os.listdir(localpath) if os.path.isfile( os.path.join(localpath, f) ) and 'root' in f ]
    # unordered comparison: the two list should be equal
    if set(localfiles) != set(output_files['lfn']):
        print "ERROR: the content of the path and the list of crab outputs are different, abort now!"
        print "localfiles (what's here locally)= ", localfiles
        print "outputfiles (what crab is saying)= ", output_files['lfn']
        raise AssertionError("CRAB3 and local list of files do not match")
#    print localpath 

    print "##### Put it all together: write this sample into the database"
    # all the info we have gathered is:
    # workArea
    # requestName
    # psetName
    # inputDataset
    # dataset_id
    # dataset_nevents (asserted to be equal to report['eventsRead'])
    # log_files
    # output_files
    # report
    # FWHash, FWBranch, FWUrl
    # AnaHash, AnaBranch, AnaUrl
    # nselected
    # localpath
    NAME = requestName + '_' + FWHash + '_' + AnaHash
    tmp_sysargv = sys.argv
    sys.argv = ["add_sample.py", "NTUPLES", localpath,
                "--name", NAME, 
                "--processed", str(dataset_nevents),
                "--nevents", str(nselected), 
# --norm
# --lumi (FIXME: figure out the fix for data whenever the tools will stabilize and be on cvmfs)
                "--code_version", AnaUrl, #NB: limited to 255 characters, but so far so good
                "--comment", FWUrl, 
                "--source_dataset", str(dataset_id),
                "--source_sample", None
# --author
# --time
                ]
    print sys.argv
    add_sample.main()
    sys.argv = tmp_sysargv

if __name__ == '__main__':
    main() 
