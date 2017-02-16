import httplib
import os
import sys
# -*- coding: utf-8 -*-

import subprocess

# import CRAB3 stuff
from CRABAPI.RawCommand import crabCommand


def retry(nattempts, exception=None):
    """
    Decorator allowing to retry an action several times before giving up.
    @params:
        nattempts  - Required: maximal number of attempts (Int)
        exception  - Optional: if given, only catch this exception, otherwise catch 'em all (Exception)
    """
    
    def tryIt(func):
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < nattempts - 1:
                try:
                    return func(*args, **kwargs)
                except (exception if exception is not None else Exception):
                    attempts += 1
            return func(*args, **kwargs)
        return wrapper
    return tryIt


@retry(5, httplib.HTTPException)
def send_crab_command(*args, **kwargs):
    """
    Send a crab command but try again (max 5 times) if server doesn't answer.
    """
    return crabCommand(*args, **kwargs)


def sum_dicts(a, b):
    """
    Sum each value of the dicts a et b and return a new dict
    """

    if len(a) == 0 and len(b) == 0:
        return {}

    if len(a) == 0:
        for key in b.viewkeys():
            a[key] = 0

    if len(b) == 0:
        for key in a.viewkeys():
            b[key] = 0

    if a.viewkeys() != b.viewkeys():
        print("Warning: files content are different. This is not a good sign, something really strange happened!")
        return None

    r = {}
    for key in a.viewkeys():
        r[key] = a[key] + b[key]

    return r


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=40):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def load_request(folder):
    """
    Return request cache from a crab task folder
    """
    
    import pickle

    cache = os.path.join(folder, '.requestcache')
    with open(cache) as f:
        cache = pickle.load(f)
        return cache


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
