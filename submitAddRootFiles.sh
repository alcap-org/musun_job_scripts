#!/bin/bash
#$ -V                              #Inherit the submission environment
#$ -l h_rt=02:00:00                # Run time (hh:mm:ss) - 2 hours
#$ -cwd                            # Start job in directory containing scripts
#$ -N prod_rn4_muPlus_a0                 # Job Name
#$ -j y                            # Combine stderr and stdout
#$ -pe 12way 24               # Requests 12 tasks/node, 24 cores total
#$ -o /scratch/02479/prindle/MCProduction/prod_rn4_muPlus_a0/Mta_pass1/$JOB_NAME.o$JOB_ID  # Name of the job output file
#$ -M prindle@uw.edu	              # Address for email notification
#$ -m be                           # Email at Begin and End of job
#$ -q normal                       # Queue name normal
set -x                             # Echo commands
ibrun /work/01491/ibanez/musun/jobscripts/MC/addRootFiles.sh /scratch/01491/ibanez/DataProduction/prod_rn4_muMinus_g0/Mu_pass4/ /scratch/01491/ibanez/DataProduction/prod_rn4_muMinus_g0/Mu_pass4/firsthalf.txt 24
