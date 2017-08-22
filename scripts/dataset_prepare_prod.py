#!/bin/env python
# coding=utf-8
"""
Ingest data from the command-line.

' '.join(['{}_{}'.format(x, y) for x in range(138, 140+1) for y in range(-31, -33-1, -1)])

138_-31 138_-32 138_-33 139_-31 139_-32 139_-33 140_-31 140_-32 140_-33

for i in  138_-031 138_-032 138_-033 139_-031 139_-032 139_-033 140_-031 140_-032 140_-033
do
    oldwofs_prepare.py --output oldwofs_${i}.yaml /g/data/fk4/wofs/current/extents/${i}/*.tif
done

"""
from __future__ import absolute_import

import uuid
from dateutil.parser import parse
import click
import netCDF4
import rasterio
import yaml
from yaml import CDumper
from datetime import datetime
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor


def prepare_datasets_netcdf(nc_file):
    """
    Don't use this, turns out the old WOfS netcdfs are of an 'alternative' structure, and can't be opened
    by GDAL/rasterio.
    """
    image = netCDF4.Dataset(nc_file)
    times = image['time']
    projection = str(image.geospatial_bounds_crs)
    left, right = float(image.geospatial_lon_min), float(image.geospatial_lon_max)
    bottom, top = float(image.geospatial_lat_min), float(image.geospatial_lat_max)    
    from_dt=datetime(2016,10,31,23,59,58)
    to_dt=datetime(2016,10,31,23,59,59)
    return { 
        'id': str(uuid.uuid4()),
        'name': prod_val,
        'product_type': prod_type_val,
        'creation_dt': parse(image.date_created).isoformat(),
        'extent': {
            'coord': {
                'ul': {'lon': left, 'lat': top},
                'ur': {'lon': right, 'lat': top},
                'll': {'lon': left, 'lat': bottom},
                'lr': {'lon': right, 'lat': bottom},
                },
                'from_dt': from_dt,
                'to_dt': to_dt,
                'center_dt': from_dt 
            },
            'format': {'name': 'NETCDF'},
            'grid_spatial': {
                'projection': {
                    'spatial_reference': projection,
                    'geo_ref_points': {
                        'ul': {'x': left, 'y': top},
                        'ur': {'x': right, 'y': top},
                        'll': {'x': left, 'y': bottom},
                        'lr': {'x': right, 'y': bottom},
                    },
                    # 'valid_data'
                }
            },
            'image': {
                'bands': {
                    'blue': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'blue'
                    },
                    'green': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'green'
                    },
                    'red': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'red'
                    },
                    'nir': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'nir'
                    },
                    'swir1': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'swir1'
                    },
                    'swir2': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'swir2'
                    }
                }
            },
            'lineage': {'source_datasets': {}},
        }


@click.command(help="Prepare datasets for indexation into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@click.option('--output', help="Write datasets into this file",
              type=click.Path(exists=False, writable=True))
@click.option('--prod', help="product name is required", default="", required=True)
@click.option('--prod_type', help="product type name like HLTC or ITEM", default="", required=True)
#@click.pass_context

def main(datasets, output, prod, prod_type):
    global prod_val
    global prod_type_val
    prod_val = prod
    prod_type_val = prod_type
    with open(output, 'w') as stream:
        with ProcessPoolExecutor(max_workers=4) as executor:
            output_datasets = executor.map(prepare_datasets_netcdf, datasets)
            with click.progressbar(output_datasets,
                                   length=len(datasets),
                                   label='Loading datasets') as progress_bar_datasets:
                    yaml.dump_all(progress_bar_datasets, stream, Dumper=CDumper)


if __name__ == "__main__":
    main()
