#!/usr/bin/env python

import unittest
import os
import subprocess
import logging
import sqlite3

formatstring=__file__ + ' : %(message)s'
logging.basicConfig(format=formatstring, level=logging.INFO)

# There are several fixures common to all tests. These include
# the dummy scripts for qsub, mu, mta, etc., a dummy database,
# and a test file output directory 

# __file__ is weird. It's equal to ./this_file_name.py, so we need
# to get the abspath of the dirname when this script is loaded, then
# return that static value with the script_path() function. 
# Otherwise, we cd somewhere_else/ and 
# dirname(__file__) = dirname(./this_file_name.py) = . = somewhere_else/
this_file = os.path.abspath(__file__)

def script_path():
  return this_file

def script_dir():
  return os.path.dirname(script_path())

logging.debug('The test directory is ' + script_dir())

os.environ['PATH'] = script_dir() + ':' + os.environ['PATH']
logging.debug('Setting PATH=' + os.environ['PATH'])

#os.environ['DATABASE'] = os.path.join(script_dir(), 'test.db')
os.environ['DATABASE'] = os.path.join(os.environ['HOME'], 'test.db')
logging.debug('Setting DATABASE=' + os.environ['DATABASE'])

os.environ['OUTPUTAREA'] = script_dir()
logging.debug('Setting OUTPUTAREA=' + os.environ['OUTPUTAREA'])

def create_proxy_database():
  """ Uses the create_proxy_database.sh script to make a test db.
  This overwrites the existing database with a new one.
  """
  subprocess.check_output('create_proxy_database.sh')

class TestBasicScripts(unittest.TestCase):
  """ Tests the simple scripts to keep them in line.
  """

  def test_qsub(self):
    """ Tests that qsub indeed runs a script. 
    
    qsub in the Lonestar submission engine sets the env variable
    JOB_ID to the job id for the running job. Since this is used
    to update the database, it's necessary for the fake qsub
    to set this variable as well.

    Since this bypasses the actual Lonestar job scheduler, 
    it should run the "fake" qsub in the test/ directory.
    """
    # qsub should always return successfully, even when the script fails
    returncode = subprocess.call(['qsub', 'true'])
    self.assertEqual(returncode, 0)

    returncode = subprocess.call(['qsub', 'false'])
    self.assertEqual(returncode, 0)

    # check that a script is actually run by qsub
    output = subprocess.check_output(['qsub', 'hello_world.sh'])
    self.assertEqual(output, 'Hello World!\n')

    # check that the script sets the JOB_ID variable for the running task
    f = open(os.path.join(script_dir(), 'fake_qsub_job_id_counter'))
    expected_output=f.read()
    output = subprocess.check_output(['qsub', 'echo_job_id.sh'])
    self.assertEqual(output, expected_output)

  def test_ibrun(self):
    """ ibrun should run N copies of a command. 
    
    N is determined by the number of cores in the parallel environment. 
    Since the test harness doesn't replicate the actual job submission 
    engine, we need to specify the number of runs through the NRUNS 
    environment variable.

    Each command should be able to access the environment variable PMI_ID
    to determine its job id.
    """
    # Check that NRUNS copies of echo_pmi_id.sh are run, each printing its PMI_ID
    os.environ['NRUNS']='12'
    expected_output=''
    for i in range(int(os.environ['NRUNS'])):
      expected_output += 'PMI_ID=' + str(i) + '\n'
    logging.debug('Expected output from ibrun is \n' + expected_output)

    output = subprocess.check_output(['ibrun', 'echo_pmi_id.sh'])
    self.assertEqual(output, expected_output)

class TestMuJobScripts(unittest.TestCase):
  """ Test harness for MuSun job scripts. See the unittest docs:
  http://docs.python.org/2/library/unittest.html
  This will test the submit*Job.sh scripts and the run*Job.sh scripts,
  so any external calls will have to be captured. This is done
  by creating empty (or minimal) bash scripts with the following names:
  mu, mta, qsub, ibrun, odbedit, response.
  """

  def setUp(self):
    """ Creates a proxy database.
    """
    create_proxy_database()

  def test_runMuJob(self):
    """ runMuJob.sh should load the odb, run mu, and write to DBUpdateFile.
    
    The argument to runMuJob.sh is a file to be sourced containing
    environment variables, ENVVARS_#, where # is the job key. 
    The following variables are set in submitMuJob.sh, with examples 
    from actual production:

      SELECTEDRUNS="69303 69304 ..."
      DATATYPE="run" # or "mc" for Monte Carlo data
      RUNKEYLIST="2489 2490 ..." # Auto-incrementing SQL key
      JOBKEY="8"
      OUTPUTDIR=/scratch/.../Mu_pass1
      RESPONSEFILES="/path/to/run69303.mid /path/to/run69304.mid ..."
      DB=/path/to/MusunProductionDataBase.db
      ODB=masterRun6.odb
      DBUPDATEFILE=/scratch/.../Mu_pass1/DBUpdateFile_8

    The blank mu script is used, so no output files are generated.
    """
    pass

  def test_submitMuJob_updates_db(self):
    """ submitMuJob.sh should run the whole MU chain.

    The database should get entries in ProductionRuns for each file
    and an entry in ProductionJobs for the submitted MU job.
    """
    create_proxy_database()
    os.chdir(os.path.join(script_dir(),'..'))
    out = subprocess.check_output(['submitMuJob.sh','-d','ds_test_proxy','-n','12'])
    logging.info(out)
    
    # Check that the number of rows is correct
    conn = sqlite3.connect(os.environ['DATABASE'])
    c = conn.cursor()

    c.execute('''SELECT COUNT(*) FROM ProductionJobs''')
    nrows = int(c.fetchone()[0])
    self.assertEquals(nrows,1)

    c.execute('''SELECT COUNT(*) FROM ProductionRuns''')
    nrows = int(c.fetchone()[0])
    self.assertEquals(nrows,12)

    c.execute('''SELECT * FROM datasets''')
    row = c.fetchone()
    self.assertEquals(row[0], 'ds_test_proxy')
    self.assertEquals(row[1], os.path.join(script_dir(),'ds_test_proxy'))


if __name__ == '__main__':
  suite1 = unittest.TestLoader().loadTestsFromTestCase(TestMuJobScripts)
  suite2 = unittest.TestLoader().loadTestsFromTestCase(TestBasicScripts)
  all_tests = unittest.TestSuite([suite1, suite2])
  #unittest.TextTestRunner(verbosity=2).run(all_tests)
  unittest.TextTestRunner(verbosity=2).run(suite1)
  #unittest.main()
