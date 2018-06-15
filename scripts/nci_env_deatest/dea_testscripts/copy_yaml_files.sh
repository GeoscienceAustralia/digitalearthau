#!/usr/bin/env bash

HOMEDIR="$1"

if [ "$1" == "--help" ] || [ "$#" -ne 1 ] || [ "$1" == "-help" ]; then
  echo "       Usage: $(basename "$0") [--help] [TEST_BASE_PATH]
                 where:
                       TEST_BASE_PATH  Test script base path
                         (ex. g/data/u46/users/sm9911)"
  echo
  exit 0
fi

# Remove previous stored config files
[ -d "$HOMEDIR/output_files" ] && rm -r "$HOMEDIR/output_files"
[ -d "$HOMEDIR/ingest_configfiles" ] && rm -r "$HOMEDIR/ingest_configfiles"
[ -d "$HOMEDIR/stats_configfiles" ] && rm -r "$HOMEDIR/stats_configfiles"
[ -d "$HOMEDIR/fc_configfiles" ] && rm -r "$HOMEDIR/fc_configfiles"

# Create new empty directory
mkdir -p "$HOMEDIR"/ingest_configfiles
mkdir -p "$HOMEDIR"/stats_configfiles
mkdir -p "$HOMEDIR"/fc_configfiles

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
                          
## Declare fractiona cover array of yaml files to download
declare -a fc_yaml_array=("ls5_fc_albers.yaml"
                          "ls7_fc_albers.yaml"
                          "ls8_fc_albers.yaml")

## Declare datacube stats array of yaml files to download
declare -a fcstats_yaml_array=("fc_stats_annual.yaml"
                               "fc_percentile_albers_seasonal.yaml"
                               "fc_percentile_albers_annual.yaml"
                               "fc_percentile_albers.yaml")

declare -a nbarstats_yaml_array=("nbar_stats.yaml")

# Replace NBAR/NBART/PQ product output location in the yaml file
cd "$HOMEDIR"/ingest_configfiles || exit 0
for i in "${dc_yaml_array[@]}"
do
  INGEST_CONF_DIR="https://github.com/opendatacube/datacube-core/raw/develop/docs/config_samples/ingester/$i"
  yaml_filename=$(basename "$INGEST_CONF_DIR")
  
  wget -q "$INGEST_CONF_DIR"
  sed -e 's,location: .*,location: "'"$HOMEDIR"'/output_files/idx_ingest/001",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done

# Replace fractional cover product output location in the yaml file
cd "$HOMEDIR"/fc_configfiles || exit 0
for i in "${fc_yaml_array[@]}"
do
  FC_CONF_DIR="https://github.com/GeoscienceAustralia/fc/raw/master/config/$i"
  yaml_filename=$(basename "$FC_CONF_DIR")
  
  wget -q "$FC_CONF_DIR"
  sed -e 's,location: .*,location: "'"$HOMEDIR"'/output_files/fc/001",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done

# Replace FC stats product output location in the yaml file
cd "$HOMEDIR"/stats_configfiles || exit 0
for i in "${fcstats_yaml_array[@]}"
do
  FC_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/fc/$i"
  yaml_filename=$(basename "$FC_STATS_CONF_DIR")
  
  wget -q "$FC_STATS_CONF_DIR"
  sed -e 's,location: .*,location: "'"$HOMEDIR"'/output_files/stats/fc/001",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done

# Replace NBAR stats product output location in the yaml file
cd "$HOMEDIR"/stats_configfiles || exit 0
for i in "${nbarstats_yaml_array[@]}"
do
  NBAR_STATS_CONF_DIR="https://github.com/GeoscienceAustralia/datacube-stats/raw/master/configurations/nbar/$i"
  yaml_filename=$(basename "$NBAR_STATS_CONF_DIR")
  
  wget -q "$NBAR_STATS_CONF_DIR"
  sed -e 's,location: .*,location: "'"$HOMEDIR"'/output_files/stats/nbar/001",' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
  sed -e 's,  start_date: .*,  start_date: 2018-01-01,' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
  sed -e 's,  end_date: .*,  end_date: 2019-01-01,' "$yaml_filename" > "$yaml_filename.tmp" && mv "$yaml_filename.tmp" "$yaml_filename"
done

cp "$HOMEDIR"/dea_testscripts/landsat_seasonal_mean.yaml "$HOMEDIR"/stats_configfiles
