#!/usr/bin/env bash

set -eu

# Find datasets with more than one location.
# Writes to STOUT

time psql -h 130.56.244.227 -p 6432 datacube -c "\\copy (
        select *
        FROM (
              SELECT
                d.id,
                array_agg(substring(dl.uri_body from 3)) AS locations,
                d.archived
              FROM agdc.dataset d INNER JOIN agdc.dataset_location dl ON d.id = dl.dataset_ref
              where d.archived is null
              GROUP BY 1, 3
        ) as dataset_locations where array_length(locations, 1) > 1
    ) to stdout with csv"


