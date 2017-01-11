#!/usr/bin/env bash

IFS="
"

for year in `seq $2 $3`; do
  for line in `datacube dataset search "${year}-01-01<time<$[$year + 1]-01-01" product=$1 | tail -n +2`; do
    filename=`echo $line | cut -d ',' -f 3 | cut -c 8- | tr -d '\r'`
    db_id=`echo $line | cut -d ',' -f 1`
    file_id=`grep '^id:' "$filename" | cut -d ' ' -f 2`
    if [ "$db_id" != "$file_id" ]; then
      echo $db_id $file_id $filename
    fi
  done
done
