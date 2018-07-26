#!/usr/bin/env bash

# Module to test
MODULE="$1"
HOMEDIR=$(pwd)
modname=$(echo "$MODULE" | sed -r "s/[/]+/_/g")
WORKDIR="$HOMEDIR"/test"$modname"-$(date '+%d%m%yT%H%M')

CONFIGFILE="$HOMEDIR/datacube_config.conf"
DCCONF="datacube_config.conf"

if [ "$1" == "--help" ] || [ "$#" -ne 1 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [DEA_MODULE_TO_TEST]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503 or dea-env or dea)"
  echo
  exit 0
fi

# Function to wait till pbs job is completed
wait_pbs_job() 
{

   pbs_qstatus=$(qstat -u "$USER")
   while [ ! -z "$pbs_qstatus" ] && [ "$pbs_qstatus" != " " ]
   do
        echo 
        echo "$(date '+%F-%T'): $1 job is executing in Raijin System....."
        sleep 60s
        pbs_qstatus=$(qstat -u "$USER")
   done

}

# Copy yaml files from repo
sh dea_testscripts/copy_yaml_files.sh "$WORKDIR"

# Get database name from the config file
configname=""
databasename=""
while IFS=': ' read -r a b; do
    if [[ "$a" == "["* ]]; then
       configname=$a
    fi

    if [[ "$a" == "db_database" && "$configname" == "[datacube]" ]]; then
       databasename="$b"
    fi
done < "$CONFIGFILE"

# Check if output directory exists, else create one
[ -d "$WORKDIR/work/ingest/001" ] || mkdir -p "$WORKDIR"/work/ingest/001
[ -d "$WORKDIR/work/stats/001" ] || mkdir -p "$WORKDIR"/work/stats/001
[ -d "$WORKDIR/work/fc/001" ] || mkdir -p "$WORKDIR"/work/fc/001
[ -d "$WORKDIR/work/nbconvert" ] || mkdir -p "$WORKDIR"/work/nbconvert

echo "
===================================================================
|  Module under test : $MODULE      
==================================================================="
echo ""
# shellcheck source=/dev/null
source "$HOMEDIR"/dea_testscripts/setup_deamodule_env.sh "$MODULE" "$DCCONF"

# Delete previous database (If any)
dea-test-env -C "$CONFIGFILE" teardown
sleep 5s

# Create a database file for testing purpose
createdb -h agdcdev-db.nci.org.au -p 6432 "$databasename"

##################################################################################################
# Run a test notebook convert using PBS
##################################################################################################
echo "
===================================================================
|  Run NB Convert using requirements_met.ipynb via PBS job        |
==================================================================="
echo ""

cd "$WORKDIR/work/nbconvert" || exit 0
qsub -v MUT="$MODULE",TEST_BASE="$WORKDIR",DC_CONF="$DCCONF", "$HOMEDIR"/dea_testscripts/pbs_nbconvert_script.sh 

##################################################################################################
# Run a test index and ingest on NCI and Raijin system
##################################################################################################
echo "
===================================================================
|  Test the following on Raijin system using PBS:                 |
|  1) Indexing  Telemetry, Level1 Scene,                          |
|     LS8_OLITIRS_NBAR/NBART/PQ Scenes for the year 2018          |
|  2) And then ingesting the products to test database            |
==================================================================="
echo ""

cd "$WORKDIR/work/ingest" || exit 0
sh "$HOMEDIR"/dea_testscripts/pbs_script.sh "$MODULE" "$CONFIGFILE" "$WORKDIR" "$databasename"

# Wait till nbar/nbart/pq albers job is completed
wait_pbs_job "NBAR/NBART/PQ Albers"

##################################################################################################
# Run a test FC using PBS (after ingest)
##################################################################################################
echo "
===================================================================
|  Test the FC on Raijin system:                                  |
|  1) Run FC on  LS8_OLITIRS_NBAR/NBART Scenes                    |
==================================================================="
echo ""
# This is required as FC code expects user to configure work root
# else work root shall be defaulted to '/g/data/v10/work/' folder
export DEA_WORK_ROOT=$WORKDIR/work/fc
export DATACUBE_CONFIG_PATH="$CONFIGFILE"
##################################################################################################
# Submit a LS8_FC job to Raijin
##################################################################################################
SUBMISSION_LOG="$WORKDIR"/work/fc/fc-$(date '+%F-%T').log
cd "$WORKDIR/work/fc" || exit 0

# Read agdc datasets from the database before Fractional Cover process
{
echo "
********************************************************************
   Datacube Config Path (FC):  $DATACUBE_CONFIG_PATH
   DEA WORK ROOT (FC):  $DEA_WORK_ROOT
********************************************************************

Read previous agdc_dataset product names and count before fractional cover process"
psql -h agdcdev-db.nci.org.au -p 6432  -d "$databasename"  -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
datacube-fc submit --app-config "$WORKDIR"/fc_configfiles/ls8_fc_albers.yaml -P u46 -q express -C "$CONFIGFILE" -vvv --year 2018 --tag 2018

sleep 60s  

# Wait till FC job is completed
wait_pbs_job "Fractional Cover"
} > "$SUBMISSION_LOG"

##################################################################################################
# Run a test Stats using PBS (after ingest)
##################################################################################################
echo "
===================================================================
|  Run Stats on LS8_ETM_NBAR products using PBS                   |
==================================================================="
echo ""
SUBMISSION_LOG="$WORKDIR"/work/stats/stats-$(date '+%F-%T').log

# Read agdc datasets from the database before stats process
{
echo "
**********************************************************************
Read previous agdc_dataset product names and count before stats process"
psql -h agdcdev-db.nci.org.au -p 6432  -d "$databasename"  -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
# If ncpus != 4 'qsub will throw an error: You have requested more memory per node (125.0GB) than the nodes in queue express can provide.'
export DATACUBE_CONFIG_PATH="$CONFIGFILE"
cd "$WORKDIR/work/stats" || exit 0
datacube-stats -C "$CONFIGFILE" -vvv --log-queries --qsub 'project=v10,nodes=2,ncpus=4,walltime=1h,mem=medium,queue=express,name=ls8fc_Stats,noask' "$WORKDIR"/stats_configfiles/fc_ls8_2018_medoid.yaml

# Wait till Stats job is completed
wait_pbs_job "Stats"

echo "
===================================================================
| Add new stats product to the test database                      |
==================================================================="
datacube -C "$CONFIGFILE" dataset add "$WORKDIR"/work/stats/001/*/*/*.nc
}  > "$SUBMISSION_LOG"

##################################################################################################
# Check everything is ok
##################################################################################################
echo "
===================================================================
| Check if everything worked as expected by querying db           |
==================================================================="
echo ""
PSQL_LOG="$WORKDIR"/work/Test_DEA_Module.log
psql -h agdcdev-db.nci.org.au -p 6432 -d "$databasename" -o "$PSQL_LOG" -L "$PSQL_LOG" << EOF
select * from agdc.dataset_location;
select id, metadata_type_ref, dataset_type_ref, archived, added, added_by from agdc.dataset;
select id, name, added, added_by from agdc.metadata_type;
select id, name, metadata from agdc.dataset_type;
select * from agdc.dataset_source;
select count(*) FROM agdc.dataset;
select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name;
EOF
