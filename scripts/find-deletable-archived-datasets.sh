#!/usr/bin/env bash

# Find any paths where all linked datasets were archived at least three days ago.
# Writes to file $locations_file

set -eu

module use /g/data/v10/public/modules/modulefiles
module load agdc-py3-prod
module load parallel

# Output file
# Day granularity, so multiple runs don't rebuild it. It doesn't need to be exact.
locations_file="lists/archived-locations-$(date +%Y%m%d).txt"

if [ ! -e "${locations_file}" ]; then
    echo "Loading existing path list"
    time psql -h 130.56.244.227 -p 6432 datacube -c "\\copy (select * from (
        select uri_scheme, substring(uri_body from 3),
          bool_and(archived is not null) as all_are_archived,
          max(archived) as newest_archive,
          count(*) as dataset_count
        from agdc.dataset_location dl
          inner join agdc.dataset d on dl.dataset_ref = d.id
        group by 1, 2
        ) uri_statuses where all_are_archived and newest_archive < current_timestamp - interval '3 days' and uri_scheme='file') to '${locations_file}.tmp' with csv"

    echo "Sorting"
    date
    < "${locations_file}.tmp" sort > "${locations_file}"
    rm "${locations_file}.tmp"
    date
fi

echo "Done. Written to ${locations_file}"

