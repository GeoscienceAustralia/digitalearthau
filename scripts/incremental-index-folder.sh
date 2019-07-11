#!/usr/bin/env bash

# Scan for locations in the given folder that aren't in the index, and index them.
#
# This is a layer in front of "datacube dataset add" that first tries to exclude things purely using the path.
# This can be faster than running index on the whole folder when you know almost everything is already indexed.

set -eu

module use /g/data/v10/public/modules/modulefiles
module load agdc-py3-prod
module load parallel

lists_offset="lists/"
# Day granularity, so multiple runs don't rebuild it. It doesn't need to be exact.
locations_file="${lists_offset}indexed-locations-$(date +%Y%m%d).txt"

index_queue_file="${lists_offset}to-index-$(date +%Y%m%d-%H%M%S).txt"
paths_file="${lists_offset}possible-paths-$(date +%Y%m%d-%H%M%S).txt"

if [ ! -e "${locations_file}" ]; then
    echo "Loading existing path list"
    time psql -h 130.56.244.227 -p 6432 datacube -c "\\copy (select substring(uri_body from 3) from agdc.dataset_location dl
        inner join dataset d on d.id = dl.dataset_ref where d.archived is null) to '${locations_file}.tmp' with csv"
    echo "Sorting"
    date
    < "${locations_file}.tmp" sort -u > "${locations_file}"
    rm "${locations_file}.tmp"
    date
fi
echo "Searching $# paths"

time lfs find "$@" -name ga-metadata.yaml | grep -v '.packagetmp.' | sort >> "${paths_file}"

echo "Finding missing paths"
# Find lines that only exist in paths_file
time comm -23 "${paths_file}" "${locations_file}" > "${index_queue_file}"

echo "Paths to index: $(wc -l "${index_queue_file}") (list: ${index_queue_file})"

echo
echo "Indexing..."

< "${index_queue_file}" time parallel -j 3 -m datacube -vv dataset add --auto-match

echo "Done"
