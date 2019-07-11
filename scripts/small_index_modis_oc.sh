#!/usr/bin/env bash

# The argument is the config file location
# e.g.
# ./small_index_modis_oc.sh ../../../modis_oc.conf
#
# Before running this script do a create-product;
# ./index_nci_modis_oc.py --config ../../../modis_oc.conf [PATH] create-product
# e.g. [PATH]= /g/data2/u39/public/data/modis/oc-1d-aust.v201508.recent/2016/12

# one year at a time
for i in /g/data2/u39/public/data/modis/oc-1d-aust.v201508.recent/2017/*; do
    ./index_nci_modis_oc.py --config  "$1"  index-data "$i"
done
