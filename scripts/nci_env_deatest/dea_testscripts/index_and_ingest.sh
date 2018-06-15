#!/usr/bin/env bash

# Check if we can connect to the database
datacube -C "$2" system check

# Run the initialise Test Database Script
# Dea-System Init adds all the products to the database (i.e., No need to do datacube product add)
# datacube product add ~/PycharmProjects/digitalearthau/digitalearthau/config/products/*.yaml
dea-system -C "$2" init

# Check if we can connect to the database
datacube -vv -C "$2" system check

# Index some test datasets
echo "
  ===================================================================
  |  Index datasets : $1                                             
  ==================================================================="
echo ""

# In order for the double-asterisk glob to work, the globstar option needs to be set
# To disable globstar: shopt -u globstar
shopt -s globstar

# Telemetry
cd /g/data/v10/repackaged/rawdata/0/2016 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/repackaged/rawdata/0/2017 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/repackaged/rawdata/0/2018 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml

# Level1 Scenes
cd /g/data/v10/reprocess/ls7/level1/2016 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls7/level1/2017 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls7/level1/2018 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml

cd /g/data/v10/reprocess/ls8/level1/2016 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls8/level1/2017 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml
cd /g/data/v10/reprocess/ls8/level1/2018 || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/**/ga-metadata.yaml

# LS7_ETM_NBAR/NBART, LS8_OLITIRS_NBAR/NBART Scenes
cd /g/data/u46/users/dra547/dea_odc_testing || exit 0
datacube -vv -C "$2" dataset add --auto-match ./**/ga-metadata.yaml

# Run a test ingest on NCI PC
echo "
  ===================================================================
  | Indexing Landsat 7 Surface Reflectance NBAR 25 metre, 100km tile|
  | Australian Albers Equal Area projection (EPSG:3577)             |
  ==================================================================="
echo ""
cd "$3"/ingest_configfiles/ || exit 0
## Declare datacube array of yaml files to download
declare -a dc_yaml_array=("ls8_nbart_albers.yaml"
                          "ls8_nbar_albers.yaml"
                          "ls8_pq_albers.yaml"
                          "ls7_nbart_albers.yaml"
                          "ls7_nbar_albers.yaml"
                          "ls7_pq_albers.yaml"
                          "ls5_nbar_albers.yaml"
                          "ls5_nbart_albers.yaml"
                          "ls5_pq_albers.yaml")

for i in "${dc_yaml_array[@]}"
do
  datacube -vv -C "$2" ingest -c "$i"
done

## Declare fractiona cover array of yaml files to download
declare -a fc_yaml_array=("ls5_fc_albers.yaml"
                          "ls7_fc_albers.yaml"
                          "ls8_fc_albers.yaml")

for i in "${fc_yaml_array[@]}"
do
  datacube -vv -C "$2" ingest -c "$i"
done

## Declare datacube stats array of yaml files to download
declare -a fcstats_yaml_array=("fc_stats_annual.yaml"
                               "fc_percentile_albers_seasonal.yaml"
                               "fc_percentile_albers_annual.yaml"
                               "fc_percentile_albers.yaml")

for i in "${fcstats_yaml_array[@]}"
do
  datacube -vv -C "$2" ingest -c "$i"
done

declare -a nbarstats_yaml_array=("nbar_stats.yaml")

for i in "${nbarstats_yaml_array[@]}"
do
  datacube -vv -C "$2" ingest -c "$i"
done
