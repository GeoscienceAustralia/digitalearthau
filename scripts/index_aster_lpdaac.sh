#!/usr/bin/env bash
# The argument is the config file location
# e.g.
# ./index_aster_lpdaac.sh ../../../dsg547_dev.conf
#./index_aster_lpdaac.py --config $1 index-data --product aster_l1t_vnir /g/data/v10/ASTER_AU/2018.01.01

# Remeber to do create product;
# python ./index_aster_lpdaac.py
# --config /g/data/u46/users/dsg547/dsg547_dev.conf
# create-product --product aster_l1t_vnir
# /g/data/v10/ASTER_AU/2018.01.01

module use /g/data/v10/public/modules/modulefiles
module load dea

for i in /g/data/u46/users/aj9439/aster/tests/*; do
    ./index_aster_lpdaac.py --config "$1" create-vrt --product aster_l1t_swir "$i"
    ./index_aster_lpdaac.py --config "$1" index-data --product aster_l1t_swir "$i"
done