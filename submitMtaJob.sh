#!/bin/bash

# This script is used for running mta. It requires a valid dataset from mu.
# We also need a valid passIn. If it is not given on command line but there is only one possibility
# in ProductionJobs we use that. If there is more than one possibility we suggest which to use.
# Note tht for mu the input file location depends on if it is real data or Monte Carlo.
# For mta that is not the case.


# Terminology:
#    Job is the unit we submit to SGE, typically analyzing a dataset.
#    Run is the individual run of a dataset. May have thousands of these per job.
#    This is the distinction in the sqlite database between ProductionJobs and ProductionRuns

# We look for mta in $MUTRUNK/src/uiuc/macros/EventTree/. Make sure MUTRUNK exists and there is an executable there.
if [ -z "${MUTRUNK+xxx}" ]; then
    echo >&2 "Did not find environment variable MUTRUNK. I expect mta to be in \$MUTRUNK/src/uiuc/macros/EventTree"
    exit 1;
fi
if [ ! -f $MUTRUNK/src/uiuc/macros/EventTree/mta ]; then
    echo >&2 "Did not find \$MUTRUNK/src/uiuc/macros/EventTree/mta. How can I run mta?"
    exit 1;
fi
if [ ! -x $MUTRUNK/src/uiuc/macros/EventTree/mta ]; then
    echo >&2 "\$MUTRUNK/src/uiuc/macros/EventTree/mta is not executable. How can I run mta?"
    exit 1;
fi


if [ -z "${DATABASE+xxx}" ]; then
    DB=`pwd`/MusunProductionDataBase.db
else
    DB=$DATABASE
fi

unset DATASETNAME
unset NRUNS
unset PASSIN
unset PASSOUT
unset PRINTHELP
COMMENT="No Comment"

while getopts ":c:d:n:p:t:m:h" opt; do
    case $opt in
        c)
            COMMENT=${OPTARG}
            ;;
        d)
            DATASETNAME=${OPTARG}
            ;;
        m)
            MC=${OPTARG}
            ;;
        n)
            NRUNS=${OPTARG}
            ;;
        p)
            PASSIN=${OPTARG}
            ;;
        t)
            PASSOUT=${OPTARG}
            ;;
        h)
            PRINTHELP=true
            ;;
        ?)
            echo "Don't understand option -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument" >&2
            exit 1
            ;;
    esac
done


if [ -n "$PRINTHELP" ]; then
    echo Usage:  submitMtaJob.sh -d datasetName -m MC -n maxRuns -p passIn -t passOut -c comment
    echo '        MC, maxRuns, passIn and passOut are optional.'
    echo '        If MC not set to something we assume we are analyzing real data'
    echo '        default value of maxRuns is the number of runs in the ProductionRuns table'
    echo '        default value of passIn is the value in ProductionJobs for this dataset that have been run through mu.'
    echo '        If there is more than one choice for passIn we prompt (or use value on command line).'
    echo '        default value of passOut is passIn.'
    echo '        If passOut has had a partial run before we add to it.'
    echo dataset is one of:    `sqlite3 $DB "SELECT DISTINCT datasetName FROM ProductionJobs WHERE jobType='mu'"`
    exit 0
fi

#############################################################################3
if [ -z "${MC+xxx}" ]; then
    DATATYPE=run
    OUTPUTAREA=$SCRATCH/DataProduction
else
    DATATYPE=MC
    OUTPUTAREA=$SCRATCH/MCProduction
fi

#############################################################################3
# Require datasetName
if [ -z "${DATASETNAME+xxx}" ]; then
    echo "datasetName name is required. (use -h to get help)" >&2
    exit 1
fi

#############################################################################3
# Make sure dataset name exists in ProductionJobs.
if [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) AS n FROM ProductionJobs WHERE datasetName='$DATASETNAME' AND jobType='mu'"` ]; then
    echo Did not find dataset named $DATASETNAME.  >&2
    echo dataset is one of:    `sqlite3 $DB "SELECT datasetName FROM ProductionJobs"` >&2
    exit 1
fi

#############################################################################3
# If PASSIN not specified look in ProductionJobs for jobType="mu". Select last one as default.
if [ -z "${PASSIN+xxx}" ]; then
    PASSES=`sqlite3 $DB "SELECT DISTINCT passOut FROM ProductionJobs WHERE dataSetName='$DATASETNAME' AND jobType='mu' ORDER BY passOut"`
    PASSARRAY=(${PASSES// / })
    if [ ${#PASSARRAY[@]} -eq 0 ]; then
        echo Did not find any jobs for $DATASETNAME that have been run through mu
        exit 1
    elif [ ${#PASSARRAY[@]} -eq 1 ]; then
        PASSIN=${PASSARRAY[0]}
    else
        echo Found multiple possible tags: $PASSES
        PASSIN=${PASSARRAY[${#PASSARRAY[@]} - 1]}
        read -e -p "Enter pass from mu (suggested value $PASSIN): " PASSIN
# Don't know why following line does not work at lonestar. Works on my computer.
#        read -e -p "Enter new passTag (or hit return to use default): " -i ${PASSARRAY[${#PASSARRAY[@]} - 1]} PASSIN
    fi
fi

#############################################################################3
# If PASSOUT not specified use PASSIN.
# If PASSOUT _is_ specified check that it is not being used with a different 
if [ -z "${PASSOUT+xxx}" ]; then
    PASSOUT=$PASSIN
fi
echo "PASSOUT = $PASSOUT,  PASSIN = $PASSIN"
# Check that we don't already have a row with this PASSOUT and conflicting PASSIN.
NCONFLICT=`sqlite3 $DB "SELECT COUNT(*) as N FROM ProductionJobs WHERE jobType='mta' AND datasetName='$DATASETNAME' AND passOut=$PASSOUT and passIn!=$PASSIN"`
if [ "$NCONFLICT" -gt "0" ]; then
    echo "We already have a ProductionJob with passOut = $PASSOUT but passIn != $PASSIN"
    exit 1
fi


#############################################################################3
# Require runs in ProductionRuns associated with entry in ProductionJobs with jobType='mu'
# Added status to ProductionJobs table so require status='Y' before ignoring run as being done in a previous pass.
# 
RUNLIST=`sqlite3 $DB "SELECT  runNumber FROM ProductionRuns,ProductionJobs
    WHERE
        ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND ProductionJobs.jobType='mu' AND
        ProductionJobs.passOut='$PASSIN' AND ProductionRuns.status='Y'
    AND ProductionRuns.runNumber NOT IN 
       (SELECT runNumber from ProductionRuns,ProductionJobs WHERE ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND
        ProductionJobs.jobType='mta' AND ProductionJobs.passOut='$PASSOUT' AND ProductionRuns.status='Y')"`
RUNLISTARRAY=(${RUNLIST// / })
NUMRUNSLEFT=${#RUNLISTARRAY[@]}
#############################################################################3
# If we did not find any appropriate runs lets see if we can give the user insight as to why.
if [ "$NUMRUNSLEFT" -eq "0" ]; then
    # Check if $DATASETNAME exists, then if there is any Response output (this just as a service to confused users of this program.)
    if [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME'"` ]; then
        echo "Did not find any runs in ProductionJobs from dataset $DATASETNAME" >&2
        exit 1
    elif [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME' AND jobType='mu'"` ]; then
        echo "Found ProductionJobs from $DATASETNAME but none of jobType=mu" >&2
        exit 1
    elif [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME' AND jobType='mu' AND passOut='$PASSIN'"` ]; then
        echo "Found ProductionJobs from $DATASETNAME with jobType=mu but none with passOut=$PASSIN" >&2
        exit 1
    fi
    echo "Don't find anymore Production jobs from $DATASETNAME that have been run through mu with passOut="$PASSIN". Must have analyzed them all (or none were run)." >&2
    exit 1
fi


#############################################################################3
# If we specified a number of runs and it is smaller than NRESPONSERUNSLEFT we use it.
if [ -z "${NRUNS+xxx}" ]; then
    NRUNS=$NUMRUNSLEFT
else
    if [ $NUMRUNSLEFT -lt $NRUNS ]; then
        NRUNS=$NUMRUNSLEFT
    fi
fi
# We were requiring a multiple of 12 runs (thats how many cores per node and we get charged for the node).
# However this is kind of a pain. Print a warning, just in case we forget and keep asking for one run per job.
# Make sure we ask for a multiple of 12 runs.
if [ $(($NRUNS%12)) -ne "0" ]; then
    echo "You have asked for $NRUNS (not a multiple of 12) out of $NUMRUNSLEFT runs left to process. We will go ahead anyway."
    NSLOTS=$[NRUNS/12+1]
    NSLOTS=$[NSLOTS*12]
else
    echo "Found $NUMRUNSLEFT runs to process and will process $NRUNS (a multiple of 12) of them"
    NSLOTS=$NRUNS
fi
# For testing we rely on NRUNS being an environment variable
export NRUNS

SELECTEDRUNS=${RUNLISTARRAY[@]:0:$NRUNS}

#############################################################################3
# Create directory if necessary
OUTPUTDIR=${OUTPUTAREA}/${DATASETNAME}/Mta_pass${PASSOUT}
mkdir -p $OUTPUTDIR
if [ $? -ne 0 ] ; then
    echo "Can't make directory $OUTPUTDIR. Quitting."
    exit 1
fi

#############################################################################3
# Update database for this job.
# Two steps. 1) insert row for run. get runKey.   2) update name incorporating runKey.

# Build up all row creation commands so we can wrap in BEGIN/COMMIT, significant speedup when modifying database.
JOBKEY=`sqlite3 $DB "INSERT INTO ProductionJobs VALUES(null,'$DATASETNAME','$PASSIN','$PASSOUT',datetime(),'','$OUTPUTDIR','','mta','$COMMENT'); SELECT seq FROM sqlite_sequence WHERE name='ProductionJobs'"`
I=0
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=mta_"${JOBKEY}_${DATATYPE}${RUNLISTARRAY[$I]}"
    echo "INSERT INTO ProductionRuns VALUES(null,'${RUNLISTARRAY[$I]}','$JOBKEY','$OUTPUTFILENAME','',datetime(),'','U'); SELECT seq FROM sqlite_sequence WHERE name='ProductionRuns';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
RUNKEYLIST=`sqlite3 $DB < $TEMPFILE`
# Removed temporary SQLite command file
rm $TEMPFILE
echo "Inserted rows in db file. Now to update name with key"

# Now we add keys to file names.
RUNKEYLISTARRAY=(${RUNKEYLIST// / })
I=0
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=mta_"${JOBKEY}_${RUNKEYLISTARRAY[$I]}_${DATATYPE}${RUNLISTARRAY[$I]}"
    echo "UPDATE ProductionRuns SET fileName='$OUTPUTFILENAME.root' WHERE runKey='${RUNKEYLISTARRAY[$I]}';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
sqlite3 $DB < $TEMPFILE
# Removed temporary SQLite command file
rm $TEMPFILE
echo "Filenames updated. Extract files names for shell scripts."

#############################################################################3
# Extract mu filenames so we can pass them on to the scripts.
I=0
MUFILES=""
while [ "$I" -lt "$NRUNS" ]; do
    if [ "$I" -lt "$NSLOTS" ]; then
        RESULT=`sqlite3 $DB "SELECT  directory,fileName from ProductionRuns,ProductionJobs
                WHERE ProductionRuns.runNumber='${RUNLISTARRAY[$I]}' AND ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND ProductionJobs.jobType='mu' AND ProductionJobs.passOut='$PASSIN' AND ProductionRuns.status='Y'"`
        RESULTARR=(${RESULT//|/ })
        MUFILES="$MUFILES ${RESULTARR[0]}/${RESULTARR[1]}"
    else
        MUFILES="$MUFILES / "
    fi

    I=$(($I+1))
done
DBUPDATEFILE=$OUTPUTDIR/DBUpdateFile_$JOBKEY
echo "Job with jobKey = $JOBKEY ready for file creation and submission."

#############################################################################3
# Exporting environment variables doesn't seem to work
# (I guess qsub doesn't see the exported variables?)
# Write to a file we can source
echo SELECTEDRUNS=\"$SELECTEDRUNS\" 1> $OUTPUTDIR/ENVVARS_$JOBKEY
echo RUNKEYLIST=\"$RUNKEYLIST\"     >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo JOBKEY=$JOBKEY                 >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo OUTPUTDIR=$OUTPUTDIR           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo MUFILES=\"$MUFILES\"           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DB=$DB                         >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DATATYPE=$DATATYPE             >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DBUPDATEFILE=$DBUPDATEFILE     >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo "BEGIN TRANSACTION; " >> $DBUPDATEFILE

#############################################################################3
# Write script for submission of job.
 echo "#!/bin/bash"                                                                     1> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -V                              #Inherit the submission environment"          >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -l h_rt=02:00:00                # Run time (hh:mm:ss) - 2 hours"              >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -cwd                            # Start job in directory containing scripts"  >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -N $DATASETNAME                 # Job Name"                                   >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -j y                            # Combine stderr and stdout"                  >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -pe 12way $NSLOTS               # Requests 12 tasks/node, $NSLOTS cores total" >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -o $OUTPUTDIR/\$JOB_NAME.o\$JOB_ID  # Name of the job output file"            >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -M ibanez4510@gmail.com	  # Address for email notification"             >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -m be                           # Email at Begin and End of job"              >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -q normal                       # Queue name normal"                          >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "set -x                             # Echo commands"                              >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "ibrun `pwd`/runMtaJob.sh $OUTPUTDIR/ENVVARS_$JOBKEY"                             >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "sqlite3 $DB \"UPDATE ProductionJobs SET endTime=datetime(), jobId='\$JOB_ID' WHERE jobKey='$JOBKEY'\"" >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "echo COMMIT\; >> $DBUPDATEFILE"                                                  >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "sqlite3 $DB < $DBUPDATEFILE"                                                     >> $OUTPUTDIR/submitJob_$JOBKEY.sh

#############################################################################3
# Actually start the job.
echo "About to submit job from $OUTPUTDIR"
 cd $OUTPUTDIR
 qsub submitJob_$JOBKEY.sh
