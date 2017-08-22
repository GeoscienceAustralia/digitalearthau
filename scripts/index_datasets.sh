# script for loading new products and datasets
module load agdc-py3-prod/1.4.1
IDIR="/g/data/u46/users/bxb547/INDEX_SCRIPTS/"
cd $IDIR
echo "adding product details"
#datacube product add ITEM_2_0_0.yaml
#datacube product add ITEM_CONF_2_0_0.yaml
datacube product add HTC_2_0_0.yaml
datacube product add LTC_2_0_0.yaml
datacube product add HTC_CNT_2_0_0.yaml
datacube product add LTC_CNT_2_0_0.yaml

#mkdir -p /g/data2/fk4/ITEM_2_0_0 -p /g/data2/fk4/ITEM_2_0_0 -p /g/data2/fk4/ITEM_2_0_0/config 
#echo "creating HLTC directories "
#mkdir -p /g/data2/fk4/HLTC_2_0_0 -p /g/data2/fk4/HLTC_2_0_0/LTC -p /g/data2/fk4/HLTC_2_0_0/LTC/config
#mkdir -p /g/data2/fk4/HLTC_2_0_0/HTC -p /g/data2/fk4/HLTC_2_0_0/HTC/config


echo "preparing and adding datasets "
#python dataset_prepare_prod.py --output /g/data2/fk4/ITEM_2.0.0/config/rel_all.yaml /g/data2/fk4/ITEM_2.0.0/ITEM_REL_*.tif
#python dataset_prepare_prod.py --output /g/data2/fk4/ITEM_2.0.0/config/offset_all.yaml /g/data2/fk4/ITEM_2.0.0/ITEM_OFFSET_*.tif

#datacube dataset add -t ITEM_2_0_0 /g/data2/fk4/ITEM_2.0.0/config/rel_all.yaml
#datacube dataset add -t ITEM_CONF_2_0_0 /g/data2/fk4/ITEM_2.0.0/config/offset_all.yaml

python dataset_prepare_prod.py  --output /g/data2/fk4/HLTC_2_0_0/LTC/config/low_all.yaml --prod "low_tide_comp_20p" --prod_type "HLTC" /g/data/fk4/HLTC_2_0_0/LTC/COMPOSITE_LOW_*.nc
datacube dataset add -t low_tide_comp_20p /g/data2/fk4/HLTC_2_0_0/LTC/config/low_all.yaml

python dataset_prepare_prod.py  --output /g/data2/fk4/HLTC_2_0_0/HTC/config/high_all.yaml --prod "high_tide_comp_20p" --prod_type "HLTC" /g/data/fk4/HLTC_2_0_0/HTC/COMPOSITE_HIGH_*.nc
datacube dataset add -t high_tide_comp_20p /g/data2/fk4/HLTC_2_0_0/HTC/config/high_all.yaml

echo "doing for count layers"

python dataset_prepare_prod.py  --output /g/data2/fk4/HLTC_2_0_0/LTC/config/low_count.yaml --prod "low_tide_comp_count" --prod_type "HLTC" /g/data2/fk4/HLTC_2_0_0/LTC/COUNT*.nc
datacube dataset add -t low_tide_comp_count /g/data2/fk4/HLTC_2_0_0/LTC/config/low_count.yaml

python dataset_prepare_prod.py  --output /g/data2/fk4/HLTC_2_0_0/HTC/config/high_count.yaml --prod "high_tide_comp_count" --prod_type "HLTC" /g/data2/fk4/HLTC_2_0_0/HTC/COUNT*.nc
datacube dataset add -t high_tide_comp_count /g/data2/fk4/HLTC_2_0_0/HTC/config/high_count.yaml
