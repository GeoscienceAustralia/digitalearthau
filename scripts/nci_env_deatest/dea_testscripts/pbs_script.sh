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

## Send email when job is aborted/begins/ends
#PBS -M santosh.mohan@ga.gov.au
#PBS -m abe

## The job will be executed from current working directory instead of home.
## PBS -l wd

## Paths for outputs and Error files
#PBS -e /g/data/u46/users/sm9911/output_files/idx_ingest
#PBS -o /g/data/u46/users/sm9911/output_files/idx_ingest

#PBS -N Test_Ingest

## Export all environment vairables in the qsub command environment to be exported to the 
## batch job
#PBS -V

source $FL_PATH/python-env/setup_default.sh $DC_CONF
sh $FL_PATH/dea_testscripts/index_and_ingest.sh $MUT $CONFIG_FILE $FL_PATH > $FL_PATH/output_files/idx_ingest/PBS_Raijin.log
