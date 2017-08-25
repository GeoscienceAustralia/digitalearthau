#!/bin/env python
# coding=utf-8
"""
Index data from the command-line.

     dataset_prepare_prod.py --output <prod>.yaml /g/data/fk4/HLTC/LTC/COMPO*.nc
done

"""
from __future__ import absolute_import

import uuid
from dateutil.parser import parse
import click
import netCDF4
import rasterio
import yaml
import itertools
from yaml import CDumper
from datetime import datetime
from pathlib import Path
from functools import partial
from concurrent.futures import ProcessPoolExecutor


def prepare_datasets_netcdf(nc_file, prod_val, prod_type_val):

    image = netCDF4.Dataset(nc_file)
    projection = str(image.geospatial_bounds_crs)
    left, right = float(image.geospatial_lon_min), float(image.geospatial_lon_max)
    bottom, top = float(image.geospatial_lat_min), float(image.geospatial_lat_max)
    from_dt = datetime(2016, 10, 31, 23, 59, 58)
    to_dt = datetime(2016, 10, 31, 23, 59, 59)
    gl_dict = {
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
            }
        }
    }
    if prod_val == "item_v2":
        loc_dict = {
            'image': {
                'bands': {
                    'relative': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'relative',
                        }
                    }
                }
            }
        return dict(itertools.chain(gl_dict.items(), loc_dict.items()))
    elif prod_val == "item_v2_conf":
        loc_dict = {
            'image': {
                'bands': {
                    'stddev': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'stddev',
                        }
                    }
                }
            }
        return dict(itertools.chain(gl_dict.items(), loc_dict.items()))
    elif "count" in prod_val:
        loc_dict = {
            'image': {
                'bands': {
                    'count_observations': {
                        'path': str(Path(nc_file).absolute()),
                        'layer': 'count_observations',
                        }
                    }
                }
            }
        return dict(itertools.chain(gl_dict.items(), loc_dict.items()))
    else:  # assuming product has six bands
        loc_dict = {
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
        return dict(itertools.chain(gl_dict.items(), loc_dict.items()))


@click.command(help="Prepare datasets for indexation into the Data Cube.")
@click.argument('datasets',
                type=click.Path(exists=True, readable=True),
                nargs=-1)
@click.option('--output', help="Write datasets into this file",
              type=click.Path(exists=False, writable=True))
@click.option('--prod', help="product name is required", default="", required=True)
@click.option('--prod_type', help="product type name like HLTC or ITEM", default="", required=True)
def main(datasets, output, prod, prod_type):
    prepare_func = partial(prepare_datasets_netcdf, prod_val=prod, prod_type_val=prod_type)
    with open(output, 'w') as stream:
        with ProcessPoolExecutor(max_workers=4) as executor:
            output_datasets = executor.map(prepare_func, datasets)
            with click.progressbar(output_datasets,
                                   length=len(datasets),
                                   label='Loading datasets') as progress_bar_datasets:
                yaml.dump_all(progress_bar_datasets, stream, Dumper=CDumper)


if __name__ == "__main__":
    main()
