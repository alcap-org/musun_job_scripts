#!/bin/bash

# This script is used to run mta on data that have been run through mu.
# It is called many times by ibrun, each invocation distinguished by PMI_ID.
source $1
STARTTIME=`sqlite3 $DB "select datetime()"`

RUNLISTARRAY=(${SELECTEDRUNS// / })

# Get next run to analyze, PMI_ID of list generated before we started any runs for this job
# Since we have blocks of 12 tasks we may not have an input file for this task.
NRUNS=${#RUNLISTARRAY[@]}
if [ $PMI_ID -ge $NRUNS ]; then
    exit 0
fi
RUNNUMBER=${RUNLISTARRAY[$PMI_ID]}

RUNKEYLISTARRAY=(${RUNKEYLIST// / })
RUNKEY=${RUNKEYLISTARRAY[$PMI_ID]}

MUFILEARRAY=(${MUFILES// / })
MUFILE=${MUFILEARRAY[$PMI_ID]}

# MTA has 2 outputs, a histogram file and a MuEPair tree file
OUTPUTFILENAME="${JOBKEY}_${RUNKEY}_${DATATYPE}${RUNNUMBER}"
OUTPUTHISTFILE=${OUTPUTDIR}/mta_${OUTPUTFILENAME}.root
OUTPUTTREEFILE=${OUTPUTDIR}/muepairtree_${OUTPUTFILENAME}.root

OUTPUTLOG=${OUTPUTDIR}/mta_${OUTPUTFILENAME}.log

STATUS="Y"
echo "$MUTRUNK/src/uiuc/macros/EventTree/mta -i $MUFILE -o $OUTPUTHISTFILE -T $OUTPUTTREEFILE &> $OUTPUTLOG"
$MUTRUNK/src/uiuc/macros/EventTree/mta -i $MUFILE -o $OUTPUTHISTFILE -T $OUTPUTTREEFILE &> $OUTPUTLOG
if [ $? -ne 0 ]; then
    STATUS="N"
fi

ENDTIME=`sqlite3 $DB "select datetime()"`
echo " UPDATE ProductionRuns SET pmiId='$PMI_ID', startTime='$STARTTIME', endTime='$ENDTIME', status='$STATUS' WHERE runKey='$RUNKEY';" >> $DBUPDATEFILE
