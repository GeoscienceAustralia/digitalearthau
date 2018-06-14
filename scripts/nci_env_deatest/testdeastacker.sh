#!/usr/bin/env bash

# Module to test
MODULE="$1"
NCIUSERNAME="$2"
HOMEDIR="/g/data/u46/users/sm9911"

echo "********************************************************************"
echo "***   Executing the script from:  $HOMEDIR                          " 
echo "********************************************************************"

CONFIGFILE=$HOMEDIR/datacube_config.conf
DCCONF="datacube_config.conf"

if [ "$1" == "--help" ] || [ "$#" -ne 2 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [DEA_MODULE_TO_TEST] [NCI_SYSTEM_USERNAME]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503 or dea-env)
                       NCI_SYSTEM_USERNAME NCI/Raijin Login username (ex. sm9911)"
  echo
  exit 0
fi

# Check if output directory exists, else create one
[ -d "$HOMEDIR/output_files/idx_ingest" ] || mkdir -p "$HOMEDIR"/output_files/idx_ingest
[ -d "$HOMEDIR/output_files/stats" ] || mkdir -p "$HOMEDIR"/output_files/stats
[ -d "$HOMEDIR/output_files/fc" ] || mkdir -p "$HOMEDIR"/output_files/fc
[ -d "$HOMEDIR/output_files/nbconvert" ] || mkdir -p "$HOMEDIR"/output_files/nbconvert

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

echo
echo "==================================================================="
echo "|  Env setup_module before index andingest process : $MODULE      "
echo "==================================================================="
echo
# shellcheck source=/dev/null
source "$HOMEDIR"/python-env/setup_default.sh "$DCCONF"

##################################################################################################
# Run a test index and ingest on NCI and Raijin system
##################################################################################################
echo
echo "==================================================================="
echo "|  Test the following on NCI-VDI system:                          |"
echo "|  1) Indexing  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes |"
echo "|  2) And then ingesting the products to test database            |"
echo "==================================================================="
echo
sh "$HOMEDIR"/dea_testscripts/index_and_ingest.sh "$MODULE" "$CONFIGFILE" "$HOMEDIR" > "$HOMEDIR"/output_files/idx_ingest/Index_Ingest_NCI_PC.log

# Check if we can connect to the database
datacube -vv system check

# This is required as dea stacker code expects user to configure work root
# else work root shall be defaulted to '/g/data/v10/work/' folder
export DEA_WORK_ROOT=/g/data/u46/users/sm9911/output_files/deastacker

dea-stacker submit -vvv -P u46 -q express -C /g/data/u46/users/sm9911/datacube_config.conf --year 2018 --app-config /g/data/u46/users/sm9911/ingest_configfiles/ls7_nbart_albers_test.yaml
