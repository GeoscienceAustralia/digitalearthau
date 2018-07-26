#!/usr/bin/env bash

WORKDIR="$1"

if [ "$1" == "--help" ] || [ "$#" -ne 1 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [TEST_BASE_PATH]
                 where:
                       TEST_BASE_PATH  Test script base path
                         (ex. g/data/u46/users/sm9911)"
  echo
  exit 0
fi

# Remove previous stored config files
[ -d "$WORKDIR/work" ] && rm -r "$WORKDIR/work"
[ -d "$WORKDIR/ingest_configfiles" ] && rm -r "$WORKDIR/ingest_configfiles"
[ -d "$WORKDIR/stats_configfiles" ] && rm -r "$WORKDIR/stats_configfiles"
[ -d "$WORKDIR/fc_configfiles" ] && rm -r "$WORKDIR/fc_configfiles"

# Create new empty directory
mkdir -p "$WORKDIR"/ingest_configfiles
mkdir -p "$WORKDIR"/stats_configfiles
mkdir -p "$WORKDIR"/fc_configfiles

## Declare datacube array of yaml files to download
declare -a dc_yaml_array=("ls8_nbart_albers.yaml"
                          "ls8_nbar_albers.yaml"
                          "ls8_pq_albers.yaml")
                          
## Declare fractiona cover array of yaml files to download
declare -a fc_yaml_array=("ls8_fc_albers.yaml")

## Declare datacube ls8 fc stats yaml file to download
declare -a fcstats_yaml_array=("fc_ls8_2015_medoid.yaml")

# Replace NBAR/NBART/PQ product output location in the yaml file
cd "$WORKDIR"/ingest_configfiles || exit 0
for i in "${dc_yaml_array[@]}"
do
  INGEST_CONF_DIR="https://github.com/geoscienceAustralia/digitalearthau/raw/develop/digitalearthau/config/ingestion/$i"
  yaml_filename=$(basename "$INGEST_CONF_DIR")
  
  wget -q "$INGEST_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/ingest/001",' "$yaml_filename"
done

# Replace fractional cover product output location in the yaml file
cd "$WORKDIR"/fc_configfiles || exit 0
for i in "${fc_yaml_array[@]}"
do
  FC_CONF_DIR="https://github.com/GeoscienceAustralia/fc/raw/master/config/$i"
  yaml_filename=$(basename "$FC_CONF_DIR")
  
  wget -q "$FC_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/fc/001",' "$yaml_filename"
done

# Replace FC stats product output location in the yaml file
cd "$WORKDIR"/stats_configfiles || exit 0
for i in "${fcstats_yaml_array[@]}"
do
  FC_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/fc/$i"
  yaml_filename=$(basename "$FC_STATS_CONF_DIR")

  wget -q "$FC_STATS_CONF_DIR"
  sed -i -e 's,location: .*,location: "'"$WORKDIR"'/work/stats/001",' "$yaml_filename"
  sed -i -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_filename"
  sed -i -e 's,  end_date: .*,  end_date: 2019-01-01,' "$yaml_filename"
  sed -i -e 's,LS8_2014_FC_MEDOID,LS8_2018_FC_MEDOID,' "$yaml_filename"
  newfile=${yaml_filename//*/fc_ls8_2018_medoid.yaml}
  mv "$yaml_filename" "$newfile"
done

#cp "$WORKDIR"/../dea_testscripts/landsat_seasonal_mean.yaml "$WORKDIR"/stats_configfiles
