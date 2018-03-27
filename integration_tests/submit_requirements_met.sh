#!/bin/bash
#PBS -P u46
#PBS -q express
#PBS -l walltime=20:00:00
#PBS -l mem=20GB
#PBS -l jobfs=1GB
#PBS -l ncpus=1
#PBS -e LOGS
#PBS -o LOGS
## The job will be executed from current working directory instead of home.
#PBS -l wd

# Convert the requirements_met Jupyter notebook to a static HTML version
# using a PBS job. This is one of the tests we run to verify a new environment
# module deployment to the NCI is safe to use.
#
# The static notebook will be output into the current directory and the logs from
# PBS will be in the LOGS/ subdirectory.

set -eux
mkdir -p LOGS
module load dea
module load udunits
jupyter nbconvert --to notebook --execute requirements_met.ipynb --ExecutePreprocessor.timeout=14400
jupyter nbconvert --to html requirements_met.nbconvert.ipynb
mv -f requirements_met.nbconvert.html requirements_met-"$(date '+%Y-%m-%d')".html
rm -f requirements_met.nbconvert.ipynb
