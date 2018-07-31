#!/usr/bin/env bash

# Module to test
MODULE="$1"
YEAR="$2"
HOMEDIR=$(pwd)

echo "
  ********************************************************************
  ***   Executing the script from:  $HOMEDIR                           
  ********************************************************************" > "$HOMEDIR"/output_files/deastacker/DEA_Stacker.log

CONFIGFILE=$HOMEDIR/datacube_config.conf

if [ "$1" == "--help" ] || [ "$#" -ne 2 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [DEA_MODULE_TO_TEST] [YEAR_TO_STACK]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503 or dea-env or dea)
                       YEAR_TO_STACK <year in YYYY format> (ex. 2018)"
  echo
  exit 0
fi

# Check if output directory exists, else create one
[ -d "$HOMEDIR/output_files/idx_ingest" ] || mkdir -p "$HOMEDIR"/output_files/idx_ingest
[ -d "$HOMEDIR/output_files/deastacker" ] || mkdir -p "$HOMEDIR"/output_files/deastacker

# Function to wait till pbs job is completed
wait_pbs_job() 
{

   pbs_qstatus=$(qstat -u "$NCIUSERNAME")
   while [ ! -z "$pbs_qstatus" ] && [ "$pbs_qstatus" != " " ]
   do
        echo 
        echo "$1 job is executing in Raijin System....."
        sleep 30s
        pbs_qstatus=$(qstat -u "$NCIUSERNAME")
   done

}

echo "
  ===================================================================
  |  Env setup_module before index andingest process : $MODULE      
  ===================================================================" >> "$HOMEDIR"/output_files/deastacker/DEA_Stacker.log
echo "" >> "$HOMEDIR"/output_files/deastacker/DEA_Stacker.log

# shellcheck source=/dev/null
source "$HOMEDIR"/dea_testscripts/setup_deamodule_env.sh "$MODULE" "$CONFIGFILE"

##################################################################################################
# Run a test index and ingest on NCI and Raijin system
##################################################################################################
echo "
  ===================================================================
  |  Test the following on NCI-VDI system:                          |
  |  1) Indexing  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes |
  |  2) And then ingesting the products to test database            |
  ===================================================================" >> "$HOMEDIR"/output_files/deastacker/DEA_Stacker.log
echo "" >> "$HOMEDIR"/output_files/deastacker/DEA_Stacker.log

sh "$HOMEDIR"/dea_testscripts/index_and_ingest.sh "$MODULE" "$CONFIGFILE" "$HOMEDIR" > "$HOMEDIR"/output_files/idx_ingest/Index_Ingest_DeaStacker.log

# Check if we can connect to the database
datacube -vv system check

# This is required as dea stacker code expects user to configure work root
# else work root shall be defaulted to '/g/data/v10/work/' folder
export DEA_WORK_ROOT="$HOMEDIR"/output_files/deastacker

dea-stacker submit -vvv -P u46 -q express -C "$HOMEDIR"/datacube_config.conf --year "$YEAR" --app-config "$HOMEDIR"/ingest_configfiles/ls7_nbart_albers_test.yaml
