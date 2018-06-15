#!/usr/bin/env bash

# Module to test
MODULE="$1"
HOMEDIR=$(pwd)

echo "
  ********************************************************************
   Executing the script from:  $HOMEDIR                           
  ********************************************************************"
echo ""

CONFIGFILE=$HOMEDIR/datacube_config.conf
DCCONF="datacube_config.conf"

if [ "$1" == "--help" ] || [ "$#" -ne 1 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [DEA_MODULE_TO_TEST]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503 or dea-env or dea)"
  echo
  exit 0
fi

# Copy yaml files from repo
sh dea_testscripts/copy_yaml_files.sh "$HOMEDIR"

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
[ -d "$HOMEDIR/output_files/idx_ingest" ] || mkdir -p "$HOMEDIR"/output_files/idx_ingest
[ -d "$HOMEDIR/output_files/stats" ] || mkdir -p "$HOMEDIR"/output_files/stats
[ -d "$HOMEDIR/output_files/fc" ] || mkdir -p "$HOMEDIR"/output_files/fc
[ -d "$HOMEDIR/output_files/nbconvert" ] || mkdir -p "$HOMEDIR"/output_files/nbconvert

# Function to wait till pbs job is completed
wait_pbs_job() 
{

   pbs_qstatus=$(qstat -u "$USER")
   while [ ! -z "$pbs_qstatus" ] && [ "$pbs_qstatus" != " " ]
   do
        echo 
        echo "$1 job is executing in Raijin System....."
        sleep 30s
        pbs_qstatus=$(qstat -u "$USER")
   done

}

echo "
  ===================================================================
  |  Env setup_module before index and ingest process : $MODULE      
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
# Run a test index and ingest on NCI and Raijin system
##################################################################################################
echo "
  ===================================================================
  |  Test the following on NCI-VDI system:                          |
  |  1) Indexing  Telemetry, Level1 Scene, LS7_ETM_NBAR/NBART,      |
  |     LS8_OLITIRS_NBAR/NBART Scenes from the year 2016-2018       |
  |  2) And then ingesting the products to test database            |
  ==================================================================="
echo ""
sh "$HOMEDIR"/dea_testscripts/index_and_ingest.sh "$MODULE" "$CONFIGFILE" "$HOMEDIR" > "$HOMEDIR"/output_files/idx_ingest/Index_Ingest_NCI_PC.log

echo "
  ===================================================================
  |  Test the following on Raijin system using PBS:                 |
  |  1) Indexing  Telemetry, Level1 Scene, LS7_ETM_NBAR/NBART,      |
  |     LS8_OLITIRS_NBAR/NBART Scenes from the year 2016-2018       |
  |  2) And then ingesting the products to test database            |
  ==================================================================="
echo ""
qsub -v MUT="$MODULE",CONFIG_FILE="$CONFIGFILE",TEST_BASE="$HOMEDIR",DC_CONF="$DCCONF" "$HOMEDIR"/dea_testscripts/pbs_script.sh
qstat -u "$USER"

# Continuously check if pbs job is completed
wait_pbs_job "Index and Ingest"

##################################################################################################
# Run a test FC using PBS (after ingest)
##################################################################################################
echo "
  ===================================================================
  |  Test the FC on Raijin system:                                  |
  |  1) Run FC on  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes|
  ==================================================================="
echo ""
# This is required as FC code expects user to configure work root
# else work root shall be defaulted to '/g/data/v10/work/' folder
export DEA_WORK_ROOT=$HOMEDIR/output_files/fc

##################################################################################################
# Submit a LS7_FC job to Raijin
##################################################################################################
datacube-fc submit --app-config "$HOMEDIR"/fc_configfiles/ls7_fc_albers.yaml -P u46 -q express -C "$CONFIGFILE" -vvv
qstat -u "$USER"

# Continuously check if pbs job is completed
wait_pbs_job "LS7_FC"

##################################################################################################
# Submit a LS8_FC job to Raijin
##################################################################################################
datacube-fc submit --app-config "$HOMEDIR"/fc_configfiles/ls8_fc_albers.yaml -P u46 -q express -C "$CONFIGFILE" -vv
qstat -u "$USER"

# Continuously check if pbs job is completed
wait_pbs_job "LS8_FC"

##################################################################################################
# Run a test Stats using PBS (after ingest)
##################################################################################################
echo "
  ===================================================================
  |  Run Stats on LS8_ETM_NBAR products using PBS                   |
  |    $CONFIGFILE                                                   
  |    $HOMEDIR/stats/nbar_stats.yaml                                
  ==================================================================="
echo ""
# If ncpus != 4 'qsub will throw an error: You have requested more memory per node (125.0GB) than the nodes in queue express can provide.'
datacube-stats -C "$CONFIGFILE" -vvv --log-queries --qsub 'project=u46,nodes=2,ncpus=4,walltime=1h,mem=medium,queue=express,name=Test_Stats,noask' "$HOMEDIR"/stats_configfiles/nbar_stats.yaml > "$HOMEDIR"/output_files/stats/PBS_Stats_Raijin.log
qstat -u "$USER"

# Continuously check if pbs job is completed
wait_pbs_job "Stats"

##################################################################################################
# Run a test notebook convert using PBS
##################################################################################################
echo "
  ===================================================================
  |  Run NB Convert using requirements_met.ipynb via PBS job        |
  ==================================================================="
echo ""
qsub -v MUT="$MODULE",TEST_BASE="$HOMEDIR",DC_CONF="$DCCONF" "$HOMEDIR"/dea_testscripts/pbs_nbconvert_script.sh
qstat -u "$USER"

# Continuously check if pbs job is completed
wait_pbs_job "NB Convert"

echo "
  ===================================================================
  | Add new stats product to the test database                      |
  ==================================================================="
echo ""
datacube -C "$CONFIGFILE" product add "$HOMEDIR"/stats_configfiles/fc_percentile_albers.yaml
datacube -C "$CONFIGFILE" product add "$HOMEDIR"/stats_configfiles/fc_percentile_albers_annual.yaml
datacube -C "$CONFIGFILE" product add "$HOMEDIR"/stats_configfiles/fc_percentile_albers_seasonal.yaml
datacube -C "$CONFIGFILE" product add "$HOMEDIR"/stats_configfiles/landsat_seasonal_mean.yaml  
datacube -C "$CONFIGFILE" dataset add "$HOMEDIR"/output_files/stats/001/SR_N_MEAN/*.nc

##################################################################################################
# Check everything is ok
##################################################################################################
echo "
  ===================================================================
  | Check if everything worked as expected by querying db           |
  ==================================================================="
echo ""
mv "$HOMEDIR"/Test_Stats.* "$HOMEDIR"/output_files/stats
rm -f "$HOMEDIR"/output_files/Test_DEA_Module.log

PSQL_LOG="$HOMEDIR"/output_files/Test_DEA_Module.log
psql -h agdcdev-db.nci.org.au -p 6432 -d "$databasename" -o "$PSQL_LOG" -L "$PSQL_LOG" << EOF
 select * from agdc.dataset_location;
 select id, metadata_type_ref, dataset_type_ref, archived, added, added_by from agdc.dataset;
 select id, name, added, added_by from agdc.metadata_type;
 select id, name, metadata from agdc.dataset_type;
 select * from agdc.dataset_source;
 select count(*) FROM agdc.dataset;
 select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name;
EOF
