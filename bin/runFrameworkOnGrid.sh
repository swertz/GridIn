#! /bin/sh

# Prepare the CMSSW release.
# For some reasons, the sandbox is extracted into the current directory, while the CMSSW release is in its own folder
# The sandbox contains the src/ python/ and lib/ directories coming from the user CMSSW release.
# For the framework to work, we link these folders from the current directory into the CMSSW release folder.

folders="biglib lib python src"

for folder in ${folders}
do
    # Remove existing folder (empty, but in case use -f)
    rm -rf ${CMSSW_BASE}/${folder}

    # And symlink folders from sandbox to the CMSSW release
    ln -sf $PWD/${folder} ${CMSSW_BASE}/${folder}
done

# Set Python path and execute script
export PYTHONPATH="${CMSSW_BASE}/python/cp3_llbb/ExTreeMaker:.:$PYTHONPATH"

python runFrameworkOnGrid.py $@