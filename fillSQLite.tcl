package require sqlite3


sqlite3 db test.db

# This creates a table named datasets with columns dataSet,directory
# For every file that is a list of runs and non-zero we add a row to datasets, create a table named for the dataset
# with column runNumber and fill the table with runNumbers from the file list. Then we alter the table to add a column
# for fileName (which we need to figure out how ti get.)
db eval "create table datasets(datasetName TEXT PRIMARY KEY, directory TEXT)";
foreach f [glob ../lists/datasets/*.list] {
    if {[file isfile $f] && [file size $f]>0} {
        set table [file rootname [file tail $f]]
        set table [string map {- Minus} $table]
        set table [string map {+ Plus} $table]
        puts "Adding table $table to MusunProductionDataBase.db"
        db eval "insert into datasets values('$table','')";
        db eval "create table ${table}(runNumber INTEGER PRIMARY KEY)";
        db copy fail $table $f
        db eval "ALTER TABLE $table ADD COLUMN fileName TEXT"
    }
}

db eval {CREATE TABLE ProductionJobs(jobKey INTEGER PRIMARY KEY AUTOINCREMENT, datasetName TEXT, passIn TEXT, passOut TEXT,
     startTime TEXT, endTime TEXT, directory TEXT, jobId INTEGER, jobType TEXT, comment TEXT)}
db eval {CREATE TABLE ProductionRuns(runKey INTEGER PRIMARY KEY AUTOINCREMENT, runNumber INTEGER, jobKey INTEGER, fileName TEXT UNIQUE, pmiId INTEGER,
     startTime TEXT, endTime TEXT, FOREIGN KEY(jobKey) REFERENCES ProductionJobs(jobKey))}

db close



to drop table
db eval {DROP TABLE ProductionJobs}
db eval {DROP TABLE ProductionRuns}


