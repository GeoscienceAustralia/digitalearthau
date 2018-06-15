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

## The job will be executed from current working directory instead of home.
## PBS -l wd

## Paths for outputs and Error files
#PBS -e output_files/nbconvert
#PBS -o output_files/nbconvert

#PBS -N NBConvert_Test

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
  ------------------------------------------------------" > "$TEST_BASE"/output_files/nbconvert/PBS_NB_Convert.log
echo "" >> "$TEST_BASE"/output_files/nbconvert/PBS_NB_Convert.log

# Run a Notebook convert on the requirements met notebook
NBFILE="$TEST_BASE"/dea_testscripts/requirements_met.ipynb
OUTPUTDIR="$TEST_BASE"/output_files/nbconvert/requirements_met-"$(date '+%Y-%m-%d')".html
cd "$TEST_BASE" || exit 0

# Load DEA module
# shellcheck source=/dev/null
source "$TEST_BASE"/dea_testscripts/setup_deamodule_env.sh "$MUT" "$DC_CONF"

## Convert a notebook to an python script and print the stdout
## To remove code cells from the output, use templateExporter
jupyter nbconvert --to python "$NBFILE" --stdout --TemplateExporter.exclude_markdown=True

## Execute the notebook
## Cell execution timeout = 5000s, --ExecutePreprocessor.timeout=5000
## --allow-errors shall allow conversion will continue and the output from 
## any exception be included in the cell output
jupyter nbconvert --ExecutePreprocessor.timeout=5000 --to notebook --execute "$NBFILE" --allow-errors
mv -f "$TEST_BASE"/dea_testscripts/requirements_met.nbconvert.ipynb "$TEST_BASE"/output_files/nbconvert

## Finally convert using notebook to html file
jupyter nbconvert --to html "$NBFILE" --stdout > "$OUTPUTDIR"

