#!/usr/bin/env bash

# Module to test
MODULE="$1"
NCIUSERNAME="$2"
export HOMEDIR="$(pwd)"
CONFIGFILE=$HOMEDIR"/Datacube_Config.conf"

if [ "$1" == "--help" ] || [ "$#" -ne 2 ] || [ "$1" == "-help" ]; then
  echo "       Usage: `basename $0` [--help] [DEA_MODULE_TO_TEST] [NCI_SYSTEM_USERNAME]
                 where:
                       DEA_MODULE_TO_TEST  Module under test (ex. dea/20180503)
                       NCI_SYSTEM_USERNAME NCI/Raijin Login username (ex. sm9911)"
  echo
  exit 0
fi

# Get database name from the config file
configname=""
databasename=""
while IFS=': ' read a b; do
    if [[ "$a" == "["* ]]; then
       configname=$a
    fi

    if [[ "$a" == "db_database" && "$configname" == "[datacube]" ]]; then
       databasename="$b"
    fi
done < $CONFIGFILE

# Setup DEA Environment
echo
echo "==================================================================="
echo "|  Env setup before index and ingest process : $MODULE             "
echo "==================================================================="
echo
init_env="umask ${umask}; source /etc/bashrc; source /$HOMEDIR/Env_Setup.sh '$databasename' '$MODULE' '$HOMEDIR'"

# Make lenient temporarily: global bashrc/etc can reference unassigned variables.
set +u
eval "${init_env}"
set -u

# Create a database file for testing purpose
createdb -h agdcdev-db.nci.org.au -p 6432 $databasename

echo
echo "==================================================================="
echo "|  Env setup before Indexing and Ingesting                        |"
echo "==================================================================="
echo
source $HOMEDIR/python-env/setup.sh $MODULE

# Run a test ingest on NCI PC
echo
echo "==================================================================="
echo "|  Test the following on NCI-VDI system:                          |"
echo "|  1) Indexing  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes |"
echo "|  2) And then ingesting the products to test database            |"
echo "==================================================================="
echo
sh $HOMEDIR/DEA_TestScripts/Index_And_Ingest.sh "$MODULE" "$CONFIGFILE" "$HOMEDIR" > $HOMEDIR/Output_Files/Ingest/Index_Ingest_Log_NCI_Machine.log

echo
echo "==================================================================="
echo "|  Test the following on Raijin system using PBS:                 |"
echo "|  1) Indexing  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes |"
echo "|  2) And then ingesting the products to test database            |"
echo "==================================================================="
echo
ssh $NCIUSERNAME@raijin.nci.org.au "qsub -v 'MUT=$MODULE','CONFIG_FILE=$CONFIGFILE','FL_PATH=$HOMEDIR' $HOMEDIR/DEA_TestScripts/PBS_Scripts/PBS_Script.sh"
ssh $NCIUSERNAME@raijin.nci.org.au "qstat -u $NCIUSERNAME"

# Run a test FC using PBS (after ingest)
echo
echo "==================================================================="
echo "|  Test the FC on Raijin system:                                  |"
echo "|  1) Run FC on  LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes|"
echo "==================================================================="
echo
ssh $NCIUSERNAME@raijin.nci.org.au "qsub -v 'MUT=$MODULE','CONFIG_FILE=$CONFIGFILE','FL_PATH=$HOMEDIR' $HOMEDIR/DEA_TestScripts/PBS_Scripts/PBS_FC_Script.sh"
ssh $NCIUSERNAME@raijin.nci.org.au "qstat -u $NCIUSERNAME"

# Run a test Stats using PBS (after ingest)
echo
echo "==================================================================="
echo "|  Run Stats on LS8_ETM_NBAR products using PBS                    |"
echo "==================================================================="
echo
sh $HOMEDIR/DEA_TestScripts/Run_Stats.sh "$CONFIGFILE" "$HOMEDIR" "$MODULE"
ssh $NCIUSERNAME@raijin.nci.org.au "qstat -u $NCIUSERNAME"

# Run a test notebook convert using PBS
#ssh $NCIUSERNAME@raijin.nci.org.au "qsub -v 'MUT=$MODULE','FL_PATH=$HOMEDIR' $HOMEDIR/DEA_TestScripts/PBS_Scripts/PBS_NBConvert_Script.sh > $HOMEDIR/Output_Files/NBConvert/PBS_NBConvert_Raijin.log"
#ssh $NCIUSERNAME@raijin.nci.org.au "qstat -u $NCIUSERNAME"
#sh $HOMEDIR/DEA_TestScripts/PBS_Scripts/PBS_NBConvert_Script.sh > $HOMEDIR/Output_Files/NBConvert/PBS_NBConvert_Raijin.log

# Check ingest, index was all ok
#echo
#echo "==================================================================="
#echo "| Check if ingest process worked as expected by querying db       |"
#echo "==================================================================="
#echo
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select * from agdc.dataset_location;"
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select id, metadata_type_ref, dataset_type_ref, archived, added, added_by from agdc.dataset;"
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select id, name, added, added_by from agdc.metadata_type;"
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select id, name, metadata from agdc.dataset_type;"
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select * from agdc.dataset_source;"
# To check indexed and ingested products
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select count(*) FROM agdc.dataset;"
#psql -h agdcdev-db.nci.org.au -p 6432 -d $databasename -c "select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name;"
#source python-env/setup.sh
#ipython
#import datacube
#dc = datacube.Datacube(config='Datacube_Config.conf')
#print(dc)
#datasets = dc.find_datasets(product='ls8_nbar_albers')
#print(datasets)


