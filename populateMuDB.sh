#!/bin/bash

# If we want to run mta over files that have been previously run through mu and we don't have
# the mu records in the database.
# Need to add a row in ProductionRuns. If DATASETNAME already exists choose a new value for passOut.
# Need to add rows in ProductionJobs

if [ -z "${DATABASE+xxx}" ]; then
    DB=MusunProductionDataBase.db
else
    DB=$DATABASE
fi

unset DATASETNAME
unset DIRECTORY
unset RUNFILE
COMMENT="Re-populate from presious run of mu"

while getopts ":c:d:D:f:h" opt; do
    case $opt in
        c)
            COMMENT=${OPTARG}
            ;;
        d)
            DATASETNAME=${OPTARG}
            ;;
        D)
            DIRECTORY=${OPTARG}
            ;;
        f)
            RUNFILE=${OPTARG}
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
    echo Usage:  populateMuDB.sh -d datasetName -D directory -f runFileName -c comment
    echo '        datasetName is a unique name describing this dataset. '
    echo '        directory is directory containing files compising this dataset'
    echo '        runFileName is a file that contains the name of all files in this dataset.'
    echo '        This script makes a lits of all files that actually exist in directory and are'
    echo '        listed in runFileName'
    exit 0
fi

#############################################################################3
# Require datasetName
if [ -z "${DATASETNAME+xxx}" ]; then
    echo "datasetName name is required. (use -h to get help)" >&2
    exit 1
fi

#############################################################################3
# If dataset exists choose a new value for passOut.
PASSOUT=1
if [ "0" -ne `sqlite3 $DB "SELECT COUNT(*) AS n FROM datasets WHERE datasets.dataSetName='$DATASETNAME'"` ]; then
    PASSOUTLIST=`sqlite3 $DB "SELECT passOut FROM '$DATASETNAME' ORDER BY passOut"`
    PASSOUTARRAY=(${PASSOUTLIST// / })
    PASSOUTLEN=${#PASSOUTARRAY[@]}
    PASSOUT=${#PASSOUTARRAY[$PASSOUTLEN-1]}
    PASSOUT=$[PASSOUT+1]
fi

#############################################################################3
# Require directory
if [ -z "${DIRECTORY+xxx}" ]; then
    echo "directory containing data is required. (use -h to get help)" >&2
    exit 1
fi

#############################################################################3
# Make sure directory exists
if [ ! -d "$DIRECTORY" ]; then
    echo "Can't find $DIRECTORY"  >&2
    exit 1
fi

#############################################################################3
# Require RUNFILE
if [ -z "${RUNFILE+xxx}" ]; then
    echo "runFileName containing files names composing dataset is required. (use -h to get help)" >&2
    exit 1
fi

#############################################################################3
# Get contents of RUNFILE.
RUNLIST=$(cat $RUNFILE)
RUNLISTARRAY=(${RUNLIST// / })
NRUNS=${#RUNLISTARRAY[@]}
if [ $NRUNS -eq 0 ]; then
    echo "$RUNFILE contained no runs." >&2
    exit 1
fi
echo "$RUNFILE contained $NRUNS entries."

#############################################################################3
# Foreach entry in RUNFILE ask if it exists in DIRECTORY. If it does append to list.
FOUNDFILES=""
FOUNDRUNS=""
I=0
while [ "$I" -lt "$NRUNS" ]; do
    if [ -f $DIRECTORY/${RUNLISTARRAY[$I]} ]; then
        FOUNDFILES="$FOUNDFILES ${RUNLISTARRAY[$I]}"
        RUNNUMBER=$(echo ${RUNLISTARRAY[$I]} | egrep -o '[[:digit:]]{5}' | head -n1)
        FOUNDRUNS="$FOUNDRUNS $RUNNUMBER"
    fi
    I=$(($I+1))
done
FOUNDARRAY=(${FOUNDFILES// / })
RUNARRAY=(${FOUNDRUNS// / })
NFOUND=${#FOUNDARRAY[@]}
echo "Found $NFOUND files listed in $RUNFILE that are in $DIRECTORY"

#############################################################################3
# Insert row into ProductionJobs

JOBKEY=`sqlite3 $DB "INSERT INTO ProductionJobs VALUES(null,'$DATASETNAME','0','$PASSOUT',datetime(),datetime(),'$DIRECTORY','','mu','$COMMENT'); SELECT seq FROM sqlite_sequence WHERE name='ProductionJobs'"`

#############################################################################3
# Build up sqlite command to add files.

I=0
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
while [ "$I" -lt "$NFOUND" ]; do
    echo "INSERT INTO ProductionRuns VALUES(null,${RUNARRAY[$I]},${JOBKEY},'${FOUNDARRAY[$I]}',0,datetime(),datetime());" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
sqlite3 $DB < $TEMPFILE
# Removed temporary SQLite command file
rm $TEMPFILE


echo "Success. (I hope)"
