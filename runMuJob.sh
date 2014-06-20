#!/bin/bash

# This script is used to run mu on Monte Carlo events that have been run through the musun detector response simulator.
# It is called many times by ibrun, each invocation distinguished by PMI_ID.
# (In principle we can also run mu on real raw data. Just need to change the sql query.)
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

RESPONSEFILEARRAY=(${RESPONSEFILES// / })
RESPONSEFILE=${RESPONSEFILEARRAY[$PMI_ID]}

OUTPUTFILENAME="${JOBKEY}_${RUNKEY}_${DATATYPE}${RUNNUMBER}"
OUTPUTFILE=${OUTPUTDIR}/mu_${OUTPUTFILENAME}.root
OUTPUTTREE=${OUTPUTDIR}/tree_${OUTPUTFILENAME}.root
OUTPUTLOG=${OUTPUTDIR}/mu_${OUTPUTFILENAME}.log
export MIDAS_DIR=$OUTPUTDIR/${OUTPUTFILENAME}
if [ ! -d $MIDAS_DIR ]; then
    mkdir $MIDAS_DIR
fi

echo "odbedit -s 10000000 -c \"load $MUTRUNK/odb/${ODB}\""
# pipes 'n' to the odbedit command, so the MIDAS log file isn't relocated
yes n | odbedit -s 10000000 -c "load $MUTRUNK/odb/${ODB}"

STATUS="Y"
echo "$MUTRUNK/work/mu -i $RESPONSEFILE -o $OUTPUTFILE -T $OUTPUTTREE &> $OUTPUTLOG"
$MUTRUNK/work/mu -i $RESPONSEFILE -o $OUTPUTFILE -T $OUTPUTTREE &> $OUTPUTLOG
if [ $? -ne 0 ]; then
    STATUS="N"
fi

ENDTIME=`sqlite3 $DB "select datetime()"`
echo " UPDATE ProductionRuns SET pmiId='$PMI_ID', startTime='$STARTTIME', endTime='$ENDTIME', status='$STATUS' WHERE runKey='$RUNKEY';" >> $DBUPDATEFILE
