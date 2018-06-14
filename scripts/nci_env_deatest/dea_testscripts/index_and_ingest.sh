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
echo
echo "==================================================================="
echo "|  Index datasets : $1                                             "
echo "==================================================================="
echo

# In order for the double-asterisk glob to work, the globstar option needs to be set
# To disable globstar: shopt -u globstar
#shopt -s globstar

# Telemetry
cd /g/data/v10/repackaged/rawdata/0/2016 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/repackaged/rawdata/0/2017 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/repackaged/rawdata/0/2018 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml

# Level1 Scenes
cd /g/data/v10/reprocess/ls7/level1/2016 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/reprocess/ls7/level1/2017 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/reprocess/ls7/level1/2018 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml

cd /g/data/v10/reprocess/ls8/level1/2016 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/reprocess/ls8/level1/2017 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml
cd /g/data/v10/reprocess/ls8/level1/2018 || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/**/*.yaml

# NBAR Scenes
cd /g/data/u46/users/dra547/dea_odc_testing || exit 1
datacube -vv -C "$2" dataset add --auto-match ./**/*.yaml

# Run a test ingest on NCI PC
echo
echo "==================================================================="
echo "| Indexing Landsat 7 Surface Reflectance NBAR 25 metre, 100km tile|"
echo "| Australian Albers Equal Area projection (EPSG:3577)             |"
echo "==================================================================="
echo
cd "$3"/ingest_configfiles/ || exit 1
datacube -vv -C "$2" ingest -c ls7_nbar_albers.yaml
datacube -vv -C "$2" ingest -c ls8_nbar_albers.yaml
datacube -vv -C "$2" ingest -c ls7_nbart_albers.yaml
datacube -vv -C "$2" ingest -c ls8_nbart_albers.yaml
