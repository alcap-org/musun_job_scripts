#!/bin/bash

# This script is used for running G4.
# It requires a valid dataset and Monte Carlo GEANT files for all runs (you can limit the number of runs with -n argument) that
# If a previous submission had a limited number of runs the logical Monte Carlo dataset can be
# increased by using the same pass tag.
# 

# Terminology:
#    Job is the unit we submit to SGE, typically analyzing a dataset.
#    Run is the individual run of a dataset. May have thousands of these per job.
#    This is the distinction in the sqlite database between ProductionJobs and ProductionRuns

# We are inheriting environment into the batch job. Make sure the executable musun exists.
hash musun 2>/dev/null || { echo >&2 "Batch job requires musun but it's not on the path."; exit 1; }

if [ -z "${DATABASE+xxx}" ]; then
    DB=`pwd`/MusunProductionDataBase.db
else
    DB=$DATABASE
fi
echo Using database file $DB
if [ -z "${OUTPUTAREA+xxx}" ]; then
    OUTPUTAREA=$SCRATCH/MCProduction
fi
echo Using OUTPUTAREA $OUTPUTAREA

unset DATASET
unset NRUNS
unset PASS
unset MACROPATH
unset MACRONAME
unset PRINTHELP
unset COMMENT

while getopts ":c:d:n:p:s:m:h" opt; do
    case $opt in
        c)
            COMMENT=${OPTARG}
            ;;
        d)
            DATASETNAME=${OPTARG}
            ;;
        n)
            NRUNS=${OPTARG}
            ;;
        p)
            PASS=${OPTARG}
            ;;
        s)
            MACROPATH=${OPTARG}
            ;;
        m)
            MACRONAME=${OPTARG}
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
    echo Usage:  submitG4Job.sh -d dataset -n maxRuns -p pass -s macroPath -m macroName -c comment
    echo '        maxRuns and pass are optional.'
    echo '        default value of maxRuns is the number of runs in dataset'
    echo '        If pass is not specified and this dataset has an exsting pass we use that.'
    echo '        If no existing pass use default value of 1'
    echo '        If macroPath not specified use default of $WORK/Muons/G4/macros/'
    echo '        Require macroName'
    echo dataset is one of:    `sqlite3 $DB "SELECT DISTINCT datasetName FROM datasets"`
    exit 0
fi

#############################################################################3
# Require datasetName
if [ -z "${DATASETNAME+xxx}" ]; then
    echo "DATASET name is required. (use -h to get help)" >&2
    exit 1
fi


#############################################################################3
# Make sure dataset name exists
if [ "1" -ne `sqlite3 $DB "SELECT COUNT(*) AS n FROM datasets WHERE datasets.dataSetName='$DATASETNAME'"` ]; then
    echo Did not find dataset named $DATASETNAME.  >&2
    echo dataset is one of:    `sqlite3 $DB "SELECT DISTINCT datasetName FROM datasets"` >&2
    exit 1
fi


#############################################################################3
# If MACROPATH not specified use default
if [ -z "${MACROPATH+xxx}" ]; then
    MACROPATH=$WORK/Muons/G4/macros/
fi

#############################################################################3
# Require MACRONAME
if [ -z "${MACRONAME+xxx}" ]; then
    echo "macroName name is required. (use -h to get help)" >&2
    exit 1
fi


#############################################################################3
# If PASS not specified use 1 (unless it has been used, then ask user)
if [ -z "${PASS+xxx}" ]; then
    PASSES=`sqlite3 $DB "SELECT DISTINCT passOut FROM ProductionJobs WHERE dataSetName='$DATASETNAME' AND jobType='G4' ORDER BY passOut"`
    PASSARRAY=(${PASSES// / })
    if [ ${#PASSARRAY[@]} -eq "0" ]; then
        PASS=1
    else
        echo Found multiple possible tags: $PASSES
        PASS=${PASSARRAY[${#PASSARRAY[@]} - 1]}
        read -e -p "Enter pass for G4 (suggested value $PASS): " PASS
# Don't know why following line does not work at lonestar. Works on my computer.
#        read -e -p "Enter new pass (or hit return to use default): " -i ${PASSARRAY[${#PASSARRAY[@]} - 1]} PASS
    fi
fi

#############################################################################3
# Check for how many runs to analyze, either the number of runs in the dataset or
# the number of runs requested. Should be a multiple of 12.
# Note that this is a complicated query that asks for run numbers from DATASETNAME that are not in the ProductionRuns table
# related to a row in the ProductionJobs table that has same PASS and DATASETNAME we are trying to submit (and is also a G4 run).
#
# Added status to ProductionJobs table so require status='Y' before ignoring run as being done in a previous pass.
RUNLIST=`sqlite3 $DB "SELECT runNumber FROM $DATASETNAME WHERE $DATASETNAME.runNumber NOT IN
   (SELECT $DATASETNAME.runNumber from ProductionRuns JOIN $DATASETNAME USING(runNumber) JOIN ProductionJobs USING(jobKey)
     WHERE ProductionJobs.passOut='$PASS' AND ProductionJobs.datasetName='$DATASETNAME' AND ProductionJobs.jobType='G4')"`
RUNLISTARRAY=(${RUNLIST// / })
NUMRUNSLEFT=${#RUNLISTARRAY[@]}
if [ "$NUMRUNSLEFT" -eq "0" ]; then
    echo Did not find any more runs in $DATASETNAME
    exit 1
else
   echo Found $NUMRUNSLEFT runs still to do in $DATASETNAME
fi
# If we specified a number of runs and it is smaller than NRUNLEFT we use it.
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
else
    echo "Found $NUMRUNSLEFT runs to process and will process $NRUNS (a multiple of 12) of them"
fi
# For testing we rely on NRUNS being an environment variable
export NRUNS

SELECTEDRUNS=${RUNLISTARRAY[@]:0:$NRUNS}

#############################################################################3
# Create directory if necessary
OUTPUTDIR=${OUTPUTAREA}/${DATASETNAME}/G4_pass${PASS}
mkdir -p $OUTPUTDIR
if [ $? -ne 0 ] ; then
    echo "Can't make directory $OUTPUTDIR. Quitting."
    exit 1
fi

#############################################################################3
# Copy Geant macro to output directory (just for the record)
cp $MACROPATH/$MACRONAME $OUTPUTDIR

#############################################################################3
# Update database for this job
# Two steps. 1) insert row for run. get runKey.   2) update name incorporating runKey.

# Write row creation commands into file so we can wrap in BEGIN/COMMIT, significant speedup when modifying database.
JOBKEY=`sqlite3 $DB "INSERT INTO ProductionJobs VALUES(null,'$DATASETNAME','4.9.6','$PASS',datetime(),'','$OUTPUTDIR','','G4','$COMMENT'); SELECT seq FROM sqlite_sequence WHERE name='ProductionJobs'"`
# Create a row for each task keeping track of runKey.
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
I=0
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=G4_"${JOBKEY}_MC${RUNLISTARRAY[$I]}"
    echo "INSERT INTO ProductionRuns VALUES(null,'${RUNLISTARRAY[$I]}','$JOBKEY','$OUTPUTFILENAME','',datetime(),'','U'); SELECT seq FROM sqlite_sequence WHERE name='ProductionRuns';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
RUNKEYLIST=`sqlite3 $DB < $TEMPFILE`
# Removed temporary SQLite command file
rm $TEMPFILE
echo "Inserted rows in db file. Now to update name with runkey"

# Now we add keys to file names.
RUNKEYLISTARRAY=(${RUNKEYLIST// / })
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
I=0
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=G4_"${JOBKEY}_${RUNKEYLISTARRAY[$I]}_MC${RUNLISTARRAY[$I]}.root"
    echo "UPDATE ProductionRuns SET fileName='$OUTPUTFILENAME' WHERE runKey='${RUNKEYLISTARRAY[$I]}';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
sqlite3 $DB < $TEMPFILE
# Removed temporary SQLite command file
rm $TEMPFILE
echo "Filenames updated."

DBUPDATEFILE=$OUTPUTDIR/DBUpdateFile_$JOBKEY

#############################################################################3
# Exporting environment variables doesn't seem to work
# (I guess qsub doesn't see the exported variables?)
# Write to a file we can source
echo SELECTEDRUNS=\"$SELECTEDRUNS\" 1> $OUTPUTDIR/ENVVARS_$JOBKEY
echo RUNKEYLIST=\"$RUNKEYLIST\"     >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo JOBKEY=$JOBKEY                 >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo OUTPUTDIR=$OUTPUTDIR           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DB=$DB                         >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DBUPDATEFILE=$DBUPDATEFILE     >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo MACROPATH=$MACROPATH           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo MACRONAME=$MACRONAME           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo "BEGIN TRANSACTION; " >> $DBUPDATEFILE

#############################################################################3
# Write script for submission of job.
 echo "#!/bin/bash"                                                                    1> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -V                             #Inherit the submission environment"          >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -l h_rt=02:00:00               # Run time (hh:mm:ss) - 2 hours"              >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -cwd                           # Start job in directory containing scripts"  >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -N $DATASETNAME                # Job Name"                                   >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -j y                           # Combine stderr and stdout"                  >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -pe 12way $NRUNS               # Requests 12 tasks/node, $NRUNS cores total" >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -o $OUTPUTDIR/\$JOB_NAME.o\$JOB_ID   # Name of the job output file"          >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -M prindle@npl.washington.edu	 # Address for email notification"             >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -m be                          # Email at Begin and End of job"              >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "#$ -q normal                      # Queue name normal"                          >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "set -x                            # Echo commands"                              >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "ibrun `pwd`/runG4Job.sh $OUTPUTDIR/ENVVARS_$JOBKEY"                             >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "sqlite3 $DB \"UPDATE ProductionJobs SET endTime=datetime(), jobId='\$JOB_ID' WHERE jobKey='$JOBKEY'\"" >> "$OUTPUTDIR"/submitJob_"$JOBKEY".sh
 echo "echo COMMIT\; >> $DBUPDATEFILE"                                                 >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "sqlite3 $DB < $DBUPDATEFILE"                                                    >> $OUTPUTDIR/submitJob_$JOBKEY.sh

#############################################################################3
# Actually start the job.
qsub "$OUTPUTDIR"/submitJob_$JOBKEY.sh
