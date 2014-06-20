#!/bin/bash

# This script is used to generate Monte Carlo events.
# It is called many times by ibrun, each invocation distinguished by PMI_ID.
source $1
STARTTIME=`sqlite3 $DB "select datetime()"`

RUNLISTARRAY=(${SELECTEDRUNS// / })

# Get next run to analyze, PMI_ID of list generated before we started any runs for this job
# Since we have blocks of 12 tasks we may not have an input file for this task.
NRUNS=${#RUNLISTARRAY[@]}
if [ $PMI_ID -ge $NRUNS ]; then
    exit 1
fi
RUNNUMBER=${RUNLISTARRAY[$PMI_ID]}

RUNKEYLISTARRAY=(${RUNKEYLIST// / })
RUNKEY=${RUNKEYLISTARRAY[$PMI_ID]}

OUTPUTFILENAME=G4_"${JOBKEY}_${RUNKEY}_MC${RUNNUMBER}"
OUTPUTFILE=$OUTPUTDIR/$OUTPUTFILENAME

STATUS="Y"
echo "musun -p $MACROPATH -m $MACRONAME -s $PMI_ID$JOB_ID -o $OUTPUTFILE.root &> $OUTPUTFILE.log"
musun -p $MACROPATH -m $MACRONAME -s $PMI_ID$JOB_ID -o $OUTPUTFILE.root &> $OUTPUTFILE.log
# musun always seems to return non-zero. We ignore it.
#if [ $? -ne 0 ]; then
#    STATUS="N"
#fi

ENDTIME=`sqlite3 $DB "select datetime()"`
echo " UPDATE ProductionRuns SET pmiId='$PMI_ID', startTime='$STARTTIME', endTime='$ENDTIME', status='$STATUS' WHERE runKey='$RUNKEY';" >> $DBUPDATEFILE
