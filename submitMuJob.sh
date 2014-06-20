#!/bin/bash

# This script is used for running mu. It requires a valid dataset.
# We also need a valid passIn. If it is not given on command line but there is only one possibility
# In ProductionJobs we use that. If there is more than one possibility we suggest which to use.
# If the MC flag is set the input file is taken from ProductionRuns with Response in jobType of related ProductionRuns.
# Otherwise we take run from table datasetName.


# Terminology:
#    Job is the unit we submit to SGE, typically analyzing a dataset.
#    Run is the individual run of a dataset. May have thousands of these per job.
#    This is the distinction in the sqlite database between ProductionJobs and ProductionRuns

# We are inheriting environment into the batch job. Make sure the executable odbedit exists.
hash odbedit 2>/dev/null || { echo >&2 "Batch job requires odbedit but it's not on the path."; exit 1; }
# We look for mu in $MUTRUNK/work. Make sure MUTRUNK exists and there is an executable there.
if [ -z "${MUTRUNK+xxx}" ]; then
    echo >&2 "Did not find environment variable MUTRUNK. I expect mu to be in \$MUTRUNK/work"
    exit 1;
fi
if [ ! -f $MUTRUNK/work/mu ]; then
    echo >&2 "Did not find \$MUTRUNK/work/mu. How can I run mu?"
    exit 1;
fi
if [ ! -x $MUTRUNK/work/mu ]; then
    echo >&2 "\$MUTRUNK/work/mu is not executable. How can I run mu?"
    exit 1;
fi


if [ -z "${DATABASE+xxx}" ]; then
    DB=`pwd`/MusunProductionDataBase.db
else
    DB=$DATABASE
fi
if [ -z "${ODBFILE+xxx}" ]; then
    ODB=master.odb
else
    ODB=$ODBFILE
fi
if [ ! -f $MUTRUNK/odb/$ODB ]; then
    echo >&2 "Did not find \$MUTRUNK/odb/$ODB. mu probably will not run without this file"
    exit 1;
fi

# Note: If we are analyzing real data passIn makes no sense.
#       PASSIN refers to input from Response.
#       By default PASSOUT is the same, but if Mu is re-run for same Response output we need to give it a different passIn (or wipe the previous run).
#       (Can also think of it like Response.passOut = Mu.passIn)
unset DATASETNAME
unset NRUNS
unset PASSIN
unset PASSOUT
unset PRINTHELP
unset COMMENT

while getopts ":c:d:n:p:t:m:h" opt; do
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
            PASSIN=${OPTARG}
            ;;
        t)
            PASSOUT=${OPTARG}
            ;;
        m)
            MC=${OPTARG}
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
    echo Usage:  submitMuJob.sh -d datasetName -m MC -n maxRuns -p passIn -t passOut -c comment
    echo '        MC, maxRuns, passIn and passOut are optional.'
    echo '        If MC not set to something we analyze real data'
    echo '        datasetName is a name from datasets table. '
    echo '        default value of maxRuns is the number of runs in dataset (if real data) or number of runs from response.'
    echo '        default value of passIn is last value (in sqlite order) of passIn from response'
    echo '        If passIn has had a partial run before we add to it.'
    echo '        Default value of passOut is passIn. Purpose is to do a new pass of just Mu using'
    echo '        same version of data from response. Of course for data passIn is irrelevant.'
    echo dataset is one of:    `sqlite3 $DB "SELECT datasetName FROM datasets"`
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
# Make sure dataset name exists
if [ "1" -ne `sqlite3 $DB "SELECT COUNT(*) AS n FROM datasets WHERE datasets.dataSetName='$DATASETNAME'"` ]; then
    echo Did not find dataset named $DATASETNAME.  >&2
    echo dataset is one of:    `sqlite3 $DB "SELECT datasetName FROM datasets"` >&2
    exit 1
fi

#############################################################################3
# If PASS not specified look in ProductionJobs for jobType="Response" iff MC. Select last one as default.
if [ -z "${PASSIN+xxx}" ]; then
    if [ -n "$MC" ]; then
        PASSES=`sqlite3 $DB "SELECT DISTINCT passOut FROM ProductionJobs WHERE dataSetName='$DATASETNAME' AND jobType='Response' ORDER BY passOut"`
        PASSARRAY=(${PASSES// / })
        if [ ${#PASSARRAY[@]} -eq 0 ]; then
            echo Did not find any jobs for $DATASETNAME that have been run through Response
            exit 1
        elif [ ${#PASSARRAY[@]} -eq 1 ]; then
            PASSIN=${PASSARRAY[0]}
        else
            echo Found multiple possible tags: $PASSES
            read -e -p "Enter pass from response (suggested value $PASSIN): " PASSIN
# Don't know why following line does not work at lonestar. Works on my computer.
#            read -e -p "Enter new passOut from response (or hit return to use default): " -i ${PASSARRAY[${#PASSARRAY[@]} - 1]} PASSIN
        fi
    else
        # For data PASSIN isn't really relevant but it is needed for ProductionJobs table. Default to 1
        PASSIN=1
    fi
fi

#############################################################################3
# If PASSOUT not specified use PASSIN.
if [ -z "${PASSOUT+xxx}" ]; then
    PASSOUT=$PASSIN
fi
# If PASSOUT _is_ specified check that it is not being used with a conflicting PASSIN.
NCONFLICT=`sqlite3 $DB "SELECT COUNT(*) as N FROM ProductionJobs WHERE jobType='mu' AND datasetName='$DATASETNAME' AND passOut=$PASSOUT and passIn!=$PASSIN"`
if [ "$NCONFLICT" -gt "0" ]; then
    echo "We already have a ProductionJob with passOut = $PASSOUT but passIn != $PASSIN"
    exit 1
fi


#############################################################################3
# If MC job we require runs in ProductionRuns associated with entry in ProductionJobs with jobType='Response'
# Added status to ProductionJobs table so require status='Y' before ignoring run as being done in a previous pass.
# 
if [ -n "$MC" ]; then
    RUNLIST=`sqlite3 $DB "SELECT  runNumber FROM ProductionRuns,ProductionJobs
        WHERE
            ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND
            ProductionJobs.jobType='Response' AND ProductionJobs.passOut='$PASSIN' AND ProductionRuns.status='Y'
        AND ProductionRuns.runNumber NOT IN 
           (SELECT runNumber from ProductionRuns,ProductionJobs WHERE ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND
            ProductionJobs.jobType='mu' AND ProductionJobs.passIn='$PASSIN' AND ProductionRuns.status='Y')"`
    RUNLISTARRAY=(${RUNLIST// / })
    NUMRUNSLEFT=${#RUNLISTARRAY[@]}
    #############################################################################3
    # If we did not find any appropriate runs lets see if we can give the user insight as to why.
    if [ "$NUMRUNSLEFT" -eq "0" ]; then
        # Check if $DATASETNAME exists, then if there is any Response output (this just as a service to confused users of this program.)
        if [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME'"` ]; then
            echo "Did not find any runs in ProductionJobs from dataset $DATASETNAME" >&2
            exit 1
        elif [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME' AND jobType='Response'"` ]; then
            echo "Found ProductionJobs from $DATASETNAME but none of jobType=Response" >&2
            exit 1
        elif [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME' AND jobType='Response' AND passOut='$PASSIN'"` ]; then
            echo "Found ProductionJobs from $DATASETNAME with jobType=Response but none with passOut=$PASSIN" >&2
            exit 1
        fi
        echo "Don't find anymore Production jobs from $DATASETNAME that have been run through Response with passOut=$PASSIN. Must have analyzed them all." >&2
        exit 1
    fi
else
# If data job we select runs from table $DATASETNAME that are not in ProductionRuns with $PASSOUT.
# Data is in directory specified in table datasets.
    RUNLIST=`sqlite3 $DB "SELECT  runNumber FROM '$DATASETNAME'
       WHERE runNumber NOT IN 
          (SELECT runNumber from ProductionRuns,ProductionJobs WHERE ProductionRuns.jobKey=ProductionJobs.jobKey AND
           ProductionJobs.datasetName='$DATASETNAME' AND ProductionJobs.jobType='mu' AND ProductionJobs.passOut='$PASSOUT' AND ProductionRuns.status='Y')"`
    RUNLISTARRAY=(${RUNLIST// / })
    NUMRUNSLEFT=${#RUNLISTARRAY[@]}
    #############################################################################3
    # If we did not find any appropriate runs lets see if we can give the user insight as to why.
    if [ "$NUMRUNSLEFT" -eq "0" ]; then
        # Check if $DATASETNAME exists, then if there is any Response output (this just as a service to confused users of this program.)
        if [ "0" -eq `sqlite3 $DB "SELECT COUNT(*) as n FROM ProductionJobs WHERE datasetName='$DATASETNAME'"` ]; then
            echo "Did not find any runs in ProductionJobs from dataset $DATASETNAME" >&2
            exit 1
        fi
        echo "All runs in $DATASETNAME have been run through mu with passOut="$PASSOUT". Perhaps start another pass?" >&2
        exit 1
    fi
fi


#############################################################################3
# If we specified a number of runs and it is smaller than NUMRUNSLEFT we use it.
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
export NRUNS

SELECTEDRUNS=${RUNLISTARRAY[@]:0:$NRUNS}

#############################################################################3
# Create directory if necessary
OUTPUTDIR=${OUTPUTAREA}/${DATASETNAME}/Mu_pass${PASSOUT}
mkdir -p $OUTPUTDIR
if [ $? -ne 0 ] ; then
    echo "Can't make directory $OUTPUTDIR. Quitting."
    exit 1
fi

#############################################################################3
# Update database for this job
# Two steps. 1) insert row for run. get runKey.   2) update name incorporating runKey.
# Note to self: if we did a partial pass a continuation still is a separate job, so gets its own entry in the ProductionJobs table.
JOBKEY=`sqlite3 $DB "INSERT INTO ProductionJobs VALUES(null,'$DATASETNAME','$PASSIN','$PASSOUT',datetime(),'','$OUTPUTDIR','','mu','$COMMENT'); SELECT seq FROM sqlite_sequence WHERE name='ProductionJobs'"`

# Build up all row creation commands so we can wrap in BEGIN/COMMIT, significant speedup when modifying database.
if [ -n "$MC" ]; then
    FILETAG="MC"
else
    FILETAG="run"
fi
I=0
# Because the command gets quite long, write to temp file and feed that to sqlite
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
echo "About to add runs to database"
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=tree_"${JOBKEY}_${FILETAG}${RUNLISTARRAY[$I]}"
    echo "INSERT INTO ProductionRuns VALUES(null,'${RUNLISTARRAY[$I]}','$JOBKEY','$OUTPUTFILENAME','',datetime(),'','U'); SELECT seq FROM sqlite_sequence WHERE name='ProductionRuns';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT;" >> $TEMPFILE
RUNKEYLIST=`sqlite3 $DB < $TEMPFILE`
# Remove temporary file for SQLite command
rm $TEMPFILE

RESPONSEFILES=""
RUNKEYLISTARRAY=(${RUNKEYLIST// / })
I=0
# Because the command gets quite long, write to temp file and feed that to sqlite
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
echo "About to update file names."
while [ "$I" -lt "$NRUNS" ]; do
    OUTPUTFILENAME=tree_"${JOBKEY}_${RUNKEYLISTARRAY[$I]}_${FILETAG}${RUNLISTARRAY[$I]}"
    echo "UPDATE ProductionRuns SET fileName='$OUTPUTFILENAME.root' WHERE runKey='${RUNKEYLISTARRAY[$I]}';" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT;" >> $TEMPFILE
sqlite3 $DB < $TEMPFILE
# Remove temporary file for SQLite command
rm $TEMPFILE


#############################################################################3
# Extract mu filenames so we can pass them on to the scripts.
# Extracting information from database is fast.
RESPONSEFILES=""
I=0
while [ "$I" -lt "$NRUNS" ]; do
    if [ -n "$MC" ]; then
        if [ "$I" -lt "$NSLOTS" ]; then
            RESULT=`sqlite3 $DB "SELECT  directory,fileName from ProductionRuns,ProductionJobs
                    WHERE ProductionRuns.runNumber='${RUNLISTARRAY[$I]}' AND ProductionRuns.jobKey=ProductionJobs.jobKey AND ProductionJobs.datasetName='$DATASETNAME' AND ProductionJobs.jobType='Response' AND ProductionJobs.passOut='$PASSIN' AND ProductionRuns.status='Y'"`
            RESULTARR=(${RESULT//|/ })
            RESPONSEFILES="$RESPONSEFILES ${RESULTARR[0]}/${RESULTARR[1]}"
        else
            echo "Appending / to RESPONSEFILES"
            RESPONSEFILES="$RESPONSEFILES / "
        fi
    else
        if [ "$I" -lt "$NSLOTS" ]; then
            RESULT=`sqlite3 $DB "SELECT  directory,fileName from datasets,$DATASETNAME WHERE datasetName='$DATASETNAME' AND runNumber='${RUNLISTARRAY[$I]}'"`
            RESULTARR=(${RESULT//|/ })
            RESPONSEFILES="$RESPONSEFILES ${RESULTARR[0]}/${RESULTARR[1]}"
        else
            echo "Appending / to RESPONSEFILES"
            RESPONSEFILES="$RESPONSEFILES / "
        fi
    fi
    I=$(($I+1))
done
DBUPDATEFILE=$OUTPUTDIR/DBUpdateFile_$JOBKEY

#############################################################################3
# Exporting environment variables doesn't seem to work
# (I guess qsub doesn't see the exported variables?)
# Write to a file we can source
echo SELECTEDRUNS=\"$SELECTEDRUNS\"   1> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DATATYPE=\"$DATATYPE\"           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo RUNKEYLIST=\"$RUNKEYLIST\"       >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo JOBKEY=$JOBKEY                   >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo OUTPUTDIR=$OUTPUTDIR             >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo RESPONSEFILES=\"$RESPONSEFILES\" >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DB=$DB                           >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo ODB=$ODB                         >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo DBUPDATEFILE=$DBUPDATEFILE       >> $OUTPUTDIR/ENVVARS_$JOBKEY
echo "BEGIN TRANSACTION; " >> $DBUPDATEFILE

#############################################################################3
# Write script for submission of job.
 echo "#!/bin/bash"                                                                     1> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -V                             #Inherit the submission environment"           >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -l h_rt=03:00:00               # Run time (hh:mm:ss) - 3 hours"               >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -cwd                           # Start job in directory containing scripts"   >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -N $DATASETNAME                # Job Name"                                    >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -j y                           # Combine stderr and stdout"                   >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -pe 12way $NSLOTS              # Requests 12 tasks/node, $NSLOTS cores total" >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -o $OUTPUTDIR/\$JOB_NAME.o\$JOB_ID        # Name of the job output file"      >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -M ibanez4510@gmail.com        # Address for email notification"              >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -m be                          # Email at Begin and End of job"               >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "#$ -q normal                      # Queue name normal"                           >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "set -x                            # Echo commands"                               >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "ibrun `pwd`/runMuJob.sh $OUTPUTDIR/ENVVARS_$JOBKEY"                              >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "sqlite3 $DB \"UPDATE ProductionJobs SET endTime=datetime(), jobId='\$JOB_ID' WHERE jobKey='$JOBKEY'\"" >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "echo COMMIT\; >> $DBUPDATEFILE"                                                  >> $OUTPUTDIR/submitJob_$JOBKEY.sh
 echo "sqlite3 $DB < $DBUPDATEFILE"                                                     >> $OUTPUTDIR/submitJob_$JOBKEY.sh


#############################################################################3
# Actually start the job.
# Run from output directory so skim files show up in the right place.
 cd $OUTPUTDIR
 qsub submitJob_$JOBKEY.sh
