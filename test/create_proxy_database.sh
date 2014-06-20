#!/bin/bash

script_dir="`dirname \"$0\"`"
script_dir="`( cd \"$script_dir\" && pwd )`"
if [ -z "$script_dir" ]; then
  # failed to get path
  echo "Failed to get path"
  exit 1
fi

#database="$script_dir/test.db"
database="$HOME/test.db"

if [ -f "$database" ]; then
  mv "$database" "$database.bak"
fi

# Add list of datasets
sqlite3 "$database" 'CREATE TABLE datasets(datasetName TEXT PRIMARY KEY, directory TEXT)'

# Add ProductionJobs table
sqlite3 "$database" 'CREATE TABLE ProductionJobs(jobKey INTEGER PRIMARY KEY AUTOINCREMENT, datasetName TEXT, passIn TEXT, passOut TEXT, startTime TEXT, endTime TEXT, directory TEXT, jobId INTEGER, jobType TEXT, comment TEXT)'

# Add ProductionRuns table
sqlite3 "$database" 'CREATE TABLE ProductionRuns(runKey INTEGER PRIMARY KEY AUTOINCREMENT, runNumber INTEGER, jobKey INTEGER, fileName TEXT UNIQUE, pmiId INTEGER, startTime TEXT, endTime TEXT, FOREIGN KEY(jobKey) REFERENCES ProductionJobs(jobKey))'

# Import a test dataset
nruns=24
export DATABASE="$database"
# Create the midas files in a directory
dataset_dir="$script_dir/ds_test_proxy"
if [ -d "$dataset_dir" ]; then
  rm "$dataset_dir/*.mid" > /dev/null 2>&1 
else
  mkdir "$dataset_dir"
fi

dataset_list="$script_dir/ds_test_proxy.list"
if [ -f "$dataset_list" ]; then rm "$dataset_list"; fi
for i in `seq 0 $nruns`; do
  runfile=`printf "run%05d.mid" "$i"`
  echo $runfile >> "$dataset_list" 
  touch "$dataset_dir/$runfile"
done

$script_dir/../populateDB.sh -d ds_test_proxy -D "$dataset_dir" -f "$dataset_list"
