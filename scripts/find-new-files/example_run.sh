#!/usr/bin/env bash

export PGPASSWORD="your password"
export DB_HOST=130.56.244.105
export DB_PORT=6432
export DB_USER="your username"

psql -h $DB_HOST -d datacube -U $DB_USER -p $DB_PORT -q -t -w -f find_new_files.sql -v product_name="'ls7_nbart_albers'" -v from_date="'2019-01-15 00:00:00'" -v to_date="'2019-02-01 00:00:00'"