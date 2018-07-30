#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space
#PBS -l jobfs=1GB

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=20:00:00

#PBS -N Test_Ingest

## Block qsub job until it completes and report the exit value of the job
#PBS block=true

MUT="$1"
CONFIG_FILE="$2"
TESTBASE="$3"
DATABASENAME="$4"

##########################################
###      PBS job information.          ###
##########################################
SUBMISSION_LOG="$TESTBASE"/work/ingest/ingest-$(date '+%F-%T').log
echo "" > "$SUBMISSION_LOG"
echo "
  ------------------------------------------------------
   -n 'Job is running on node '; cat $PBS_NODEFILE
  ------------------------------------------------------
   PBS: qsub is running on $PBS_O_HOST
   PBS: Originating queue      = $PBS_O_QUEUE
   PBS: Executing queue        = $PBS_QUEUE
   PBS: Working directory      = $PBS_O_WORKDIR
   PBS: Execution mode         = $PBS_ENVIRONMENT
   PBS: Job identifier         = $PBS_JOBID
   PBS: Job name               = $PBS_JOBNAME
   PBS: Node_file              = $PBS_NODEFILE
   PBS: Current home directory = $PBS_O_HOME
   PBS: PATH                   = $PBS_O_PATH
  ------------------------------------------------------" >> "$SUBMISSION_LOG"
echo "" >> "$SUBMISSION_LOG"

# shellcheck source=/dev/null
source "$TESTBASE"/../dea_testscripts/setup_deamodule_env.sh "$MUT" "$CONFIG_FILE"
sh "$TESTBASE"/../dea_testscripts/index_and_ingest.sh "$MUT" "$CONFIG_FILE" "$TESTBASE" "$DATABASENAME" >> "$SUBMISSION_LOG"
