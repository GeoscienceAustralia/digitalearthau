#!/usr/bin/env bash

# Function to wait till pbs job is completed
wait_pbs_job() 
{

   pbs_qstatus=$(qstat -u "$USER")
   while [ ! -z "$pbs_qstatus" ] && [ "$pbs_qstatus" != " " ]
   do
        echo 
        echo "$(date '+%F-%T'): $1 job is executing in Raijin System....."
        sleep 30s
        pbs_qstatus=$(qstat -u "$USER")
   done

}

# Run the initialise Test Database Script
# Dea-System Init adds all the products to the database (i.e., No need to do datacube product add)
# datacube product add ~/PycharmProjects/digitalearthau/digitalearthau/config/products/*.yaml
dea-system -C "$2" init

# Check if we can connect to the database
datacube -vv -C "$2" system check

# Index some test datasets
# In order for the double-asterisk glob to work, the globstar option needs to be set
# To disable globstar: shopt -u globstar
shopt -s globstar

declare -a nbart_scene_folders=("LS8_OLITIRS_NBART_P54_GANBART01-032_088_076_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_077_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_078_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_079_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_080_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_081_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_082_20180504"
                                "LS8_OLITIRS_NBART_P54_GANBART01-032_088_083_20180504")
                             
declare -a nbar_scene_folders=("LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_076_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_077_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_078_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_079_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_080_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_081_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_082_20180504"
                               "LS8_OLITIRS_NBAR_P54_GANBAR01-032_088_083_20180504")

declare -a pq_scene_folders=("LS8_OLITIRS_PQ_P55_GAPQ01-032_088_076_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_077_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_078_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_079_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_080_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_081_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_082_20180504"
                             "LS8_OLITIRS_PQ_P55_GAPQ01-032_088_083_20180504")

# LS8_OLITIRS_NBAR/NBART/PQ Scenes
for i in "${nbart_scene_folders[@]}"
do
    sh "$3"/../dea_testscripts/dea-sync.sh "$1" 2018 ls8_nbart_scene no "/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2018/05/output/nbart/$i" "$3"
    sleep 10s
done

for i in "${pq_scene_folders[@]}"
do
    sh "$3"/../dea_testscripts/dea-sync.sh "$1" 2018 ls8_pq_scene no "/g/data/rs0/scenes/pq-scenes-tmp/ls8/2018/05/output/pqa/$i" "$3"
    sleep 10s
done

for i in "${nbar_scene_folders[@]}"
do
    cd "/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2018/05/output/nbar/$i" || exit 0
    datacube -vv -C "$2" dataset add ./**/ga-metadata.yaml
    sleep 10s
done

sleep 60s  

# Wait till sync and nbconvert job is completed
wait_pbs_job "Dea-Sync and NBConvert"

# Read agdc datasets from the database before Ingest process
echo ""
echo "**********************************************************************"
echo "Read previous agdc_dataset product names and count before Ingest process"
psql -h agdcdev-db.nci.org.au -p 6432  -d "$4"  -c 'select name, count(*) FROM agdc.dataset a, agdc.dataset_type b where a.dataset_type_ref = b.id group by b.name'
echo "**********************************************************************"
echo ""

# Run a test ingest on NCI PC
echo "
===================================================================
| Ingest Landsat 8 Surface Reflectance NBAR 25 metre, 100km tile  |
| Australian Albers Equal Area projection (EPSG:3577)             |
==================================================================="
echo ""
echo "********************************************************************"
echo "   Datacube Config Path (Ingest):  $DATACUBE_CONFIG_PATH" 
echo "********************************************************************"

## Declare datacube array of yaml files to download
declare -a albers_yaml_array=("ls8_nbart_albers.yaml"
                              "ls8_nbar_albers.yaml"
                              "ls8_pq_albers.yaml")

DC_PATH="$3"/../"$(basename "$2")"
export DATACUBE_CONFIG_PATH="$DC_PATH"

cd "$3"/work/ingest || exit 0
for i in "${albers_yaml_array[@]}"
do
  productname=$(echo "$i" | cut -f 1 -d '.')
  yes Y | dea-submit-ingest qsub --project u46 --queue express -n 5 -t 10 -m a -M santosh.mohan@ga.gov.au -W umask=33 --name ingest_"$productname" -c "$3"/ingest_configfiles/"$i" --allow-product-changes "$productname" 2018
done

sleep 60s
