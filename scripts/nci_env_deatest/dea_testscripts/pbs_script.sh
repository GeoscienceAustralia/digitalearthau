#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space. 
#PBS -l jobfs=1GB

## The number of cpus required for the job to run.
#PBS -l ncpus=16
#PBS -l walltime=1:00:00

## The job will be executed from current working directory instead of home.
## PBS -l wd

## Paths for outputs and Error files
#PBS -e output_files/idx_ingest
#PBS -o output_files/idx_ingest

#PBS -N Index_Ingest_Test

## Export all environment vairables in the qsub command environment to be exported to the 
## batch job
#PBS -V

##########################################
###      PBS job information.          ###
##########################################

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
  ------------------------------------------------------" > "$TEST_BASE"/output_files/idx_ingest/PBS_Index_Ingest.log
echo "" >> "$TEST_BASE"/output_files/idx_ingest/PBS_Index_Ingest.log

# shellcheck source=/dev/null
source "$TEST_BASE"/dea_testscripts/setup_deamodule_env.sh "$MUT" "$DC_CONF"
sh "$TEST_BASE"/dea_testscripts/index_and_ingest.sh "$MUT" "$CONFIG_FILE" "$TEST_BASE" >> "$TEST_BASE"/output_files/idx_ingest/PBS_Index_Ingest.log
