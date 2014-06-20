# This is a simple test suite for checking if database is updated appropriately by batch submission shell scripts.
# Might be nice to use an actual test harness such as tcltest.

export DATABASE=test.db
export OUTPUTAREA=./test
OLDPATH=$PATH
export PATH=./test:$PATH



# Following should create one row in ProductionJobs table, 12 in ProductionRuns table
export JOB_ID=1
./submitG4Job.sh -d prod_rn4_muMinus_d0 -n 12
echo Should now have  1 ProductionJobs row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionJobs"`
echo Should now have 12 ProductionRuns row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionRuns"`

# Following command asks for dataset that has not been processed in G4 yet. Should fail
export JOB_ID=2
./submitResponseJob.sh -d prod_rn4_muMinus_d1
echo Should now have  1 ProductionJobs row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionJobs"`
echo Should now have 12 ProductionRuns row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionRuns"`

# Following command asks for dataset pass that has not been processed in G4 yet. Should fail
export JOB_ID=3
./submitResponseJob.sh -d prod_rn4_muMinus_d0 -p 2
echo Should now have  1 ProductionJobs row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionJobs"`
echo Should now have 12 ProductionRuns row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionRuns"`

# Following should now process output of first submitG4Job.sh
export JOB_ID=4
./submitResponseJob.sh -d prod_rn4_muMinus_d0
echo Should now have  2 ProductionJobs row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionJobs"`
echo Should now have 24 ProductionRuns row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionRuns"`

# Do another twelve runs from prod_rn4_muMinus_d0
export JOB_ID=5
./submitG4Job.sh -d prod_rn4_muMinus_d0 -n 12
echo Should now have  3 ProductionJobs row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionJobs"`
echo Should now have 36 ProductionRuns row. Find `sqlite3 test.db "select COUNT(*) AS N FROM ProductionRuns"`

# Do another twelve runs from another dataset
export JOB_ID=6
./submitG4Job.sh -d prod_rn4_muPlus_a0 -n 12


# Do another pass of twelve runs from the other dataset
export JOB_ID=7
./submitG4Job.sh -d prod_rn4_muPlus_a0 -n 12 -p 2

# run response on second pass
export JOB_ID=8
./submitResponseJob.sh -d prod_rn4_muPlus_a0 -p 2

# try response on first pass with calling output pass 2. Should fail
export JOB_ID=9
./submitResponseJob.sh -d prod_rn4_muPlus_a0 -p 1 -t 2

# can run pass 1 from G4 if we call output something else, say 3
export JOB_ID=10
./submitResponseJob.sh -d prod_rn4_muPlus_a0 -p 1 -t 3

# And we can run multiple times over same G4 output with different response passes
export JOB_ID=11
./submitResponseJob.sh -d prod_rn4_muPlus_a0 -p 1 -t 1

# but if we have already analyzed data we don't do it again
export JOB_ID=12
./submitResponseJob.sh -d prod_rn4_muPlus_a0 -p 1 -t 1

# Check that Mu can update tables with output of response
export JOB_ID=13
./submitMuJob.sh -d prod_rn4_muPlus_a0 -p 2 -m G4

# Do another twelve runs from another dataset
export JOB_ID=14
./submitMuJob.sh -d prod_rn4_muPlus_a0 -p 1 -m G4

# Souldn't find something with only G4 output but not response
export JOB_ID=15
./submitG4Job.sh -d prod_rn4_muMinus_d1 -n 12
./submitMuJob.sh -d prod_rn4_muMinus_d1 -m G4


# Finally som Mta checks.
export JOB_ID=16
./submitMtaJob.sh -d prod_rn4_muPlus_a0 -p 2


PATH=$OLDPATH
