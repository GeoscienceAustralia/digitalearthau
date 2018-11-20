#!/usr/bin/env bash
# The argument is the config file location
#./index_nci_modis_lpdaac.py --config $1 index-data /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/2002.07.04

for i in /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/*; do
    ./index_nci_modis_lpdaac.py --config "$1" index-data "$i"
done