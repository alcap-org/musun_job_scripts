#!/bin/bash

if [ -z "${DATABASE+xxx}" ]; then
    DB=MusunProductionDataBase.db
else
    DB=$DATABASE
fi

unset DATASETNAME
unset DIRECTORY
unset RUNFILE
while getopts ":d:D:f:h" opt; do
    case $opt in
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
    echo Usage:  populateDB.sh -d datasetName -D directory -f runFileName
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
# Make sure dataset name does not exist
if [ "0" -ne `sqlite3 $DB "SELECT COUNT(*) AS n FROM datasets WHERE datasets.dataSetName='$DATASETNAME'"` ]; then
    echo "$DATASETNAME already exists in table datasets."  >&2
    exit 1
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
# Now create table DATASETNAME and populate it.
# The table needs to have unique run numbers. If insertion into DATASET fails print a
# message and drop the table.

sqlite3 $DB "CREATE TABLE ${DATASETNAME}(runNumber INTEGER PRIMARY KEY)";
sqlite3 $DB "ALTER TABLE $DATASETNAME ADD COLUMN fileName TEXT"

#############################################################################3
# Build up sqlite command to add files.

I=0
# Use a temporary file for the potentially long insert command
TEMPFILE=`mktemp -t mu.XXXXXXXXXX`
echo "BEGIN TRANSACTION; " >> $TEMPFILE
while [ "$I" -lt "$NFOUND" ]; do
    echo "INSERT INTO $DATASETNAME VALUES(${RUNARRAY[$I]},'${FOUNDARRAY[$I]}');" >> $TEMPFILE
    I=$(($I+1))
done
echo "COMMIT; " >> $TEMPFILE
sqlite3 $DB < $TEMPFILE
# Removed temporary SQLite command file
rm $TEMPFILE

if [ $? -ne 0 ] ; then
    echo "Problem inserting runs into $DATASETNAME. Perhaps run numbers are not unique?"
    sqlite3 $DB "DROP TABLE $DATASETNAME"
    exit 1
fi

sqlite3 $DB "INSERT INTO datasets VALUES('$DATASETNAME','$DIRECTORY')";


echo "Success. (I hope)"
