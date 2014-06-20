#!/bin/bash

# This script is used to run Monte Carlo events through the musun detector response simulator.
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

G4FILEARRAY=(${G4FILES// / })
G4FILE=${G4FILEARRAY[$PMI_ID]}

OUTPUTFILENAME=response_"${JOBKEY}_${RUNKEY}_MC${RUNNUMBER}"
OUTPUTFILE=$OUTPUTDIR/$OUTPUTFILENAME
export MIDAS_DIR=$OUTPUTFILE
if [ ! -d $MIDAS_DIR ]; then
    mkdir $MIDAS_DIR
fi

echo "odbedit -s 10000000 -c \"load $MUTRUNK/odb/${ODB}\""
# pipes 'n' to the odbedit command, so the MIDAS log file isn't relocated
yes n | odbedit -s 10000000 -c "load $MUTRUNK/odb/${ODB}"

STATUS="Y"
echo "response -s $PMI_ID$JOB_ID -i $G4FILE -o $OUTPUTFILE.mid -q ${OUTPUTFILE}_QA.root &> $OUTPUTFILE.log"
response -s $PMI_ID$JOB_ID -i $G4FILE -o $OUTPUTFILE.mid -q ${OUTPUTFILE}_QA.root &> $OUTPUTFILE.log
if [ $? -ne 0 ]; then
    STATUS="N"
fi

ENDTIME=`sqlite3 $DB "select datetime()"`
echo " UPDATE ProductionRuns SET pmiId='$PMI_ID', startTime='$STARTTIME', endTime='$ENDTIME', status='$STATUS' WHERE runKey='$RUNKEY';" >> $DBUPDATEFILE
