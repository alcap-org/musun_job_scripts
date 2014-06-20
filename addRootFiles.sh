#!/bin/bash

# This script should be passed a directory, a filename containing a list of root files to hadd and the number
# of cores this job has asked for. Read file list and take a section of the list based on PMI_ID and number of cores.

cd $1
RUNLIST=$(cat $2)
RUNLISTARRAY=(${RUNLIST// / })
NRUNS=${#RUNLISTARRAY[@]}
if [ $NRUNS -eq 0 ]; then
    echo "File $2 contained no runs." >&2
    exit 1
fi
echo "File $2 contained $NRUNS entries."

NFILESPERJOB=$[NRUNS/$3+1]
NFIRST=$[NFILESPERJOB*PMI_ID]
NLAST=$[NFILESPERJOB*(PMI_ID+1)]
if [ $NLAST -gt $NRUNS ]; then
    NLAST=$NRUNS
fi
LENGTH=$[NLAST-NFIRST]
echo "Number of files per job $NFILESPERJOB , Last file to add $NLAST"
echo "Will add $LENGTH runs starting at $NFIRST of list"

FILELIST=${RUNLISTARRAY[@]:$NFIRST:$LENGTH}
echo "hadd summedFiles_${PMI_ID}.root $FILELIST"
hadd summedFiles_${PMI_ID}.root $FILELIST > summedFiles_${PMI_ID}.log
