#!/usr/bin/env bash

### This script works only on Raijin PC

# Module to test
MODULE="$1"
HOMEDIR=$(pwd)
OUTPUTDIR="$HOMEDIR/output_files/submit_sync"
DCCONF="datacube_config.conf"

if [ "$1" == "--help" ] || [ "$#" -ne 1 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [DEA_MODULE_TO_TEST]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503 or dea-env or dea)"
  echo
  exit 0
fi

echo "
  ===================================================================
  |  Env setup_module before sync process : $MODULE                  
  ===================================================================" > "$OUTPUTDIR"/Dea_Sync_Submit.log
echo "" >> "$OUTPUTDIR"/Dea_Sync_Submit.log

# shellcheck source=/dev/null
source "$HOMEDIR"/dea_testscripts/setup_deamodule_env.sh "$MODULE" "$DCCONF"

# Check if we can connect to the database
datacube -vv system check

echo "
  ********************************************************************
  ***   Executing the script from:  $HOMEDIR                           
  ********************************************************************" >> "$OUTPUTDIR"/Dea_Sync_Submit.log

dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS5_TM_NBAR/Workdir --cache-folder "$OUTPUTDIR"/LS5_TM_NBAR/Cachedir /g/data/rs0/datacube/002/LS5_TM_NBAR/1_-11
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS5_TM_NBART/Workdir --cache-folder "$OUTPUTDIR"/LS5_TM_NBART/Cachedir /g/data/rs0/datacube/002/LS5_TM_NBART/3_-15
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS5_TM_PQ/Workdir --cache-folder "$OUTPUTDIR"/LS5_TM_PQ/Cachedir /g/data/rs0/datacube/002/LS5_TM_PQ/23_-12

dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS7_ETM_NBAR/Workdir --cache-folder "$OUTPUTDIR"/LS7_ETM_NBAR/Cachedir /g/data/rs0/datacube/002/LS7_ETM_NBAR/6_-16
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS7_ETM_NBART/Workdir --cache-folder "$OUTPUTDIR"/LS7_ETM_NBART/Cachedir /g/data/rs0/datacube/002/LS7_ETM_NBART/15_-48
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS7_ETM_PQ/Workdir --cache-folder "$OUTPUTDIR"/LS7_ETM_PQ/Cachedir /g/data/rs0/datacube/002/LS7_ETM_PQ/20_-37

dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS8_OLI_NBAR/Workdir --cache-folder "$OUTPUTDIR"/LS8_OLI_NBAR/Cachedir /g/data/rs0/datacube/002/LS8_OLI_NBAR/5_-45
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS8_OLI_NBART/Workdir --cache-folder "$OUTPUTDIR"/LS8_OLI_NBART/Cachedir /g/data/rs0/datacube/002/LS8_OLI_NBART/5_-45
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS8_OLI_PQ/Workdir --cache-folder "$OUTPUTDIR"/LS8_OLI_PQ/Cachedir /g/data/rs0/datacube/002/LS8_OLI_PQ/23_-21

dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS5_TM_FC/Workdir --cache-folder "$OUTPUTDIR"/LS5_TM_FC/Cachedir /g/data/fk4/datacube/002/FC/LS5_TM_FC/8_-49
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS7_ETM_FC/Workdir --cache-folder "$OUTPUTDIR"/LS7_ETM_FC/Cachedir /g/data/fk4/datacube/002/FC/LS7_ETM_FC/20_-39
dea-submit-sync -P u46 -q express --max-jobs 1 --work-folder "$OUTPUTDIR"/LS8_OLI_FC/Workdir --cache-folder "$OUTPUTDIR"/LS8_OLI_FC/Cachedir /g/data/fk4/datacube/002/FC/LS8_OLI_FC/12_-51
