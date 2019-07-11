#!/usr/bin/env bash
# The argument is the config file location
# e.g.
# ./index_modis_lpdaac.sh ../../../dsg547_dev.conf
#./index_nci_modis_lpdaac.py --config $1 index-data /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/2002.07.04

# Remeber to do create product;
# python ./index_nci_modis_lpdaac.py
# --config /g/data/u46/users/dsg547/dsg547_dev.conf
# create-product
# /g/data/u46/users/dsg547/test_data/lpdaac-tiles-c6/MYD13Q1.006/2017.12.11/ 

for i in /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/*; do
    ./index_nci_modis_lpdaac.py --config "$1" index-data "$i"
done
