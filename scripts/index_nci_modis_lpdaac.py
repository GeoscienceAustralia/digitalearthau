#!/usr/bin/env python
"""
This program allows indexing the CSIRO MODIS Vegetation Indices Data stored on
 the NCI into an ODC Database

It runs in two modes, one to create the product definition in the database,
 and the second to record
dataset details. Both modes need to be pointed at a directory of Vegetation
 Indices data stored in hdf format.

The data is stored in sets of hdf files every 16 days
in `/g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/`.

The script  can be run in either with either a `create_product`
or `index_data` parameter mode, and an output directory of hdf files.
 It reads the hdf files to create the Product/Dataset
definitions, and write them directly into an ODC database.

It doesn't write out intermediate YAML files, and attempts to create
stable UUIDs for the generate Datasets, based on the file path
and modification time of the underlying NetCDF Data.

::

    ./index_nci_modis_lpdaac.py create_product /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/2018.10.08
    ./index_nci_modis_lpdaac.py index_data /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/2018.10.08

::

    psql -h agdcdev-db.nci.org.au
    CREATE DATABASE modis_lpdaac WITH OWNER agdc_admin;
    GRANT TEMPORARY, CONNECT ON DATABASE modis_lpdaac to public;

modis_lpdaac.conf::

    [datacube]
    db_hostname: agdcdev-db.nci.org.au
    db_port: 6432
    db_database: modis_lpdaac

::

    datacube --config modis_lpdaac.conf system init

::

    for i in /g/data2/u39/public/data/modis/lpdaac-tiles-c6/MYD13Q1.006/*; do
        ./index_nci_modis_lpdaac.py --config modis_lpdacc.conf index_data $i
    done

"""
import json
import logging
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import click
import numpy as np
import rasterio

from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils.geometry import CRS, box


LOG = logging.getLogger(__name__)


@click.group(help=__doc__)
@click.option('--config', '-c', help="Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    """ Used to pass the datacube index to functions via click."""
    ctx.obj = Datacube(config=config).index


@cli.command()
@click.argument('path')
@click.pass_obj
def show(index, path):

    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)

    resolver = Doc2Dataset(index)
    for file_path in file_paths:
        doc = generate_lpdaac_doc(file_path)
        print_dict(doc)


@cli.command()
@click.argument('path')
@click.pass_obj
def create_product(index, path):
    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)
    measurements = raster_to_measurements(file_paths[0])
    for measure in measurements:
        measure.pop('path')  # This is not needed here
    print_dict(measurements)
    product_def = generate_lpdaac_defn(measurements)
    print_dict(product_def)

    print(index)
    product = index.products.from_doc(product_def)
    print(product)
    indexed_product = index.products.add(product)
    print(indexed_product)


@cli.command()
@click.argument('path')
@click.pass_obj
def index_data(index, path):
    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)

    resolver = Doc2Dataset(index)
    for file_path in file_paths:
        doc = generate_lpdaac_doc(file_path)
        print_dict(doc)
        dataset, err = resolver(doc, file_path.as_uri())

        if err is not None:
            logging.error("%s", err)
        try:
            index.datasets.add(dataset)
        except Exception as e:
            logging.error("Couldn't index %s%s", file_path, name)
            logging.exception("Exception", e)


def print_dict(doc):
    print(json.dumps(doc, indent=4, sort_keys=True, cls=NumpySafeEncoder))


def find_lpdaac_file_paths(path: Path):
    """
    Return a list of hdf file path objects.

    :param path:
    :return: A list of path objects.
    """
    file_paths = []
    for afile in path.iterdir():
        if afile.suffix == '.hdf' and afile.stem[:7] == 'MYD13Q1':
            file_paths.append(afile)
    return file_paths


def raster_to_measurements(file_path):
    measurements = []

    with rasterio.open(file_path, 'r') as img:
        for subdataset in img.subdatasets:
            with rasterio.open(subdataset) as sub_img:
                measure = {}
                measure['dtype'] = str(sub_img.dtypes[0])
                measure['nodata'] = float(sub_img.nodatavals[0])
                measure['units'] = str(sub_img.units[0])
                tmp = sub_img.descriptions[0].replace('250m 16 days ', '')
                tmp = tmp.replace(" ", "_")
                measure['name'] = str(tmp) # descriptions
                measure['path'] = subdataset
                measurements.append(measure)
        return measurements


def generate_lpdaac_defn(measurements):
    return {
        'name': 'modis_lpdaac_MYD13Q1',
        'metadata_type': 'eo',
        'metadata': {
            'product_type': 'modis_lpdaac_MYD13Q1',
            'platform': {'code': 'MODIS'},
            'version': 1,
            'coverage': 'aust'
        },
        'storage': {
            'crs': 'SR-ORG:6842',
            'resolution': {
                'latitude': -0.01,
                'longitude': 0.01
            }
        },
        'description': 'MODIS NDVI/EVI',
        'measurements': measurements
    }


def generate_lpdaac_doc(file_path):

    modification_time = file_path.stat().st_mtime

    unique_ds_uri = f'{file_path.as_uri()}#{modification_time}'
    with rasterio.open(file_path, 'r') as img:
        asubdataset = img.subdatasets[0]
    left, bottom, right, top, spatial_ref = get_grid_spatial_projection(asubdataset)
    geo_ref_points = {
                         'ul': {'x': left, 'y': top},
                         'ur': {'x': right, 'y': top},
                         'll': {'x': left, 'y': bottom},
                         'lr': {'x': right, 'y': bottom},
                     }

    start_time, end_time = modis_path_to_date_range(file_path)
    measurements = raster_to_measurements(file_path)
    the_format = 'HDF4_EOS:EOS_GRID'
    for m in measurements:
        m['fmt'], m['local_path'], m['layer'] = split_path(m['path'])
        assert the_format == m['fmt']

    doc = {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, unique_ds_uri)),
        'product_type': 'modis_lpdaac_MYD13Q1',
        'creation_dt': str( datetime.fromtimestamp(modification_time)),
        'platform': {'code': 'MODIS'},
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'coord': to_lat_long_extent(left, bottom, right, top,
                                        spatial_ref),
        },
        'format': {'name': the_format},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': spatial_ref,
            }
        },
        'image': {
            'bands': {
                measure['name']: {
                    'path': str(measure['local_path']),
                    'layer': measure['layer'],
                } for measure in measurements
            }
        },
    'version': 1,
    'coverage': 'aust',
    'lineage': {'source_datasets': {}}
    }
    return doc


def split_path(apath):
    splitpath =  apath.split(':')
    assert len(splitpath) > 3
    fmt = ':'.join(splitpath[:2]) # 'HDF4_EOS:EOS_GRID'
    local_path = splitpath[2]
    layer = ':'.join(splitpath[3:])
    return fmt, local_path, layer


def modis_path_to_date_range(file_path):
    a_year_days = file_path.name.split('.')[1] # example 'A2017101'
    year_days = a_year_days[1:]
    # from https:...how-do-modis-products-naming-conventions-work
    start_time = datetime.strptime(year_days, '%Y%j')

    end_time = start_time + timedelta(days=16) - timedelta(microseconds=1)
    return start_time, end_time


def name_to_date_range(name):
    date = name[1:9]
    start_time = datetime.strptime(date, '%Y%m%d')
    end_time = start_time + timedelta(days=1) - timedelta(microseconds=1)
    return start_time, end_time


def to_lat_long_extent(left, bottom, right, top, spatial_reference, new_crs="EPSG:4326"):

    crs = CRS(spatial_reference)
    abox = box(left, bottom, right, top, crs)
    projected = abox.to_crs(CRS(new_crs))
    proj = projected.boundingbox
    proj_list = [proj.left, proj.bottom, proj.right, proj.top]
    left, bottom, right, top = [round(i, 3) for i in proj_list]
    coord = {
             'ul': {'lon': left, 'lat': top},
             'ur': {'lon': right, 'lat': top},
             'll': {'lon': left, 'lat': bottom},
             'lr': {'lon': right, 'lat': bottom},
    }
    return coord


def get_grid_spatial_projection(fname):
    with rasterio.open(fname, 'r') as img:
        left, bottom, right, top = [round(i, 3) for i in img.bounds]
        spatial_reference = str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt))
        return left, bottom, right, top, spatial_reference


class NumpySafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumpySafeEncoder, self).default(obj)


if __name__ == '__main__':
    cli()
