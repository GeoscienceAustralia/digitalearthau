#!/bin/bash
#PBS -a 0500
#PBS -P u46
#PBS -l ncpus=1,walltime=00:05:00
#PBS -l wd,mem=500MB
#PBS -l other=gdata2

/g/data/u46/users/dra547/quota-tracker/track-quotas.py

qsub /g/data/u46/users/dra547/quota-tracker/track-with-pbs.sh
