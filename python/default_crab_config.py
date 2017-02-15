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
    config.JobType.disableAutomaticOutputCollection = True
    config.JobType.outputFiles = []
    config.JobType.allowUndistributedCMSSW = True
    config.JobType.sendExternalFolder = True # To send electron MVA ids with jobs

    config.Data.inputDBS = 'global'
    config.Data.allowNonValidInputDataset = True

    if is_mc:
        config.Data.splitting = 'FileBased'
    else:
        config.Data.splitting = 'LumiBased'

    config.Data.outLFNDirBase = '/store/user/%s/' % (getUsernameFromSiteDB())
    config.Data.publication = False

    config.Site.storageSite = 'T2_BE_UCL'

    return config
