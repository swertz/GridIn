__author__ = 'sbrochet'

def create_config(is_mc):
    """
    Create a default CRAB configuration suitable to run the framework
    :return:
    """

    from CRABClient.UserUtilities import config, getUsernameFromSiteDB
    config = config()

    config.General.workArea = 'tasks'
    config.General.transferOutputs = True
    config.General.transferLogs = True

    config.JobType.pluginName = 'Analysis'
    config.JobType.psetName = '../../Framework/test/TestConfiguration.py'
#    config.JobType.psetName = '../python/dummy_pset.py'
#    config.JobType.scriptExe = '../bin/runFrameworkOnGrid.sh'
    config.JobType.sendPythonFolder = True
    config.JobType.disableAutomaticOutputCollection = True
    config.JobType.allowUndistributedCMSSW = True
    config.JobType.inputFiles = ['../python/runFrameworkOnGrid.py'] #FIXME: to be removed?
    config.JobType.outputFiles = ['output_mc.root']

    config.Data.inputDBS = 'global'

    if is_mc:
        config.Data.splitting = 'FileBased'
    else:
        config.Data.splitting = 'LumiBased'

    config.Data.outLFNDirBase = '/store/user/%s/' % (getUsernameFromSiteDB())
    config.Data.publication = False

    config.Site.storageSite = 'T2_BE_UCL'

    return config
