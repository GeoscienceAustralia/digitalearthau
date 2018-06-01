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
#PBS -l walltime=20:00:00

## Send email when job is aborted/begins/ends
#PBS -M santosh.mohan@ga.gov.au
#PBS -m abe

## The job will be executed from current working directory instead of home.
## PBS -l wd

## Paths for outputs and Error files
#PBS -e /g/data/u46/users/sm9911/output_files/nbconvert
#PBS -o /g/data/u46/users/sm9911/output_files/nbconvert

#PBS -N Test_NBConvert

## Export all environment vairables in the qsub command environment to be exported to the 
## batch job
#PBS -V

echo "Running on the node: $PBS_NODEFILE"

# Run a Notebook convert on the requirements met notebook
NBFILE=$FL_PATH/dea_testscripts/requirements_met.ipynb
OUTPUTDIR=$FL_PATH/output_files/nbconvert/requirements_met-"$(date '+%Y-%m-%d')".html
cd $FL_PATH || exit 1

# Load DEA module
source $FL_PATH/python-env/setup_default.sh $DC_CONF

## Convert a notebook to an python script and print the stdout
## To remove code cells from the output, use templateExporter
jupyter nbconvert --to python $NBFILE --stdout --TemplateExporter.exclude_markdown=True

## Execute the notebook
## Cell execution timeout = 5000s, --ExecutePreprocessor.timeout=5000
## --allow-errors shall allow conversion will continue and the output from 
## any exception be included in the cell output
jupyter nbconvert --ExecutePreprocessor.timeout=5000 --to notebook --execute $NBFILE --allow-errors
mv -f $FL_PATH/dea_testscripts/requirements_met.nbconvert.ipynb $FL_PATH/output_files/nbconvert

## Finally convert using notebook to html file
jupyter nbconvert --to html $NBFILE --stdout > "$OUTPUTDIR"

