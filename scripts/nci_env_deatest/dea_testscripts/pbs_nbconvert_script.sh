#!/usr/bin/env bash
## Project name
#PBS -P u46

## Queue type
#PBS -q express

## The total memory limit across all nodes for the job
#PBS -l mem=32GB

## The requested job scratch space.
#PBS -l jobfs=1GB

## The number of cpus required for the job to run
#PBS -l ncpus=16
#PBS -l walltime=05:00:00

#PBS -N Test_NBConvert

##########################################
###      PBS job information.          ###
##########################################
##########################################
SUBMISSION_LOG="$TEST_BASE"/work/nbconvert/nbconvert-$(date '+%F-%T').log

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

# Run a Notebook convert on the requirements met notebook
NBFILE="$TEST_BASE"/../dea_testscripts/requirements_met.ipynb
OUTPUTDIR="$TEST_BASE"/work/nbconvert/requirements_met-"$(date '+%Y-%m-%d')".html
cd "$TEST_BASE" || exit 0

# Load DEA module
# shellcheck source=/dev/null
source "$TEST_BASE"/../dea_testscripts/setup_deamodule_env.sh "$MUT" "$DC_CONF"

## Convert a notebook to an python script and print the stdout
## To remove code cells from the output, use templateExporter
jupyter nbconvert --to python "$NBFILE" --stdout --TemplateExporter.exclude_markdown=True

## Execute the notebook
## Cell execution timeout = 5000s, --ExecutePreprocessor.timeout=5000
## --allow-errors shall allow conversion will continue and the output from 
## any exception be included in the cell output
jupyter nbconvert --ExecutePreprocessor.timeout=5000 --to notebook --execute "$NBFILE" --allow-errors
[ -d "$TEST_BASE"/../dea_testscripts/requirements_met.nbconvert.ipynb ] || mv -f "$TEST_BASE"/../dea_testscripts/requirements_met.nbconvert.ipynb "$TEST_BASE"/work/nbconvert

## Finally convert using notebook to html file
jupyter nbconvert --to html "$NBFILE" --stdout > "$OUTPUTDIR"

## Remove temp file
[ -d "$TEST_BASE"/../dea_testscripts/mydask.png ] || rm -f "$TEST_BASE"/../dea_testscripts/mydask.png
