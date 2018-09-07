import json
import logging
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import click
import netCDF4
import numpy as np
import rasterio

from datacube import Datacube
from datacube.index.hl import Doc2Dataset
from datacube.utils import changes

LOG = logging.getLogger(__name__)


@click.group()
@click.option('--config', '-c', help=" Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    ctx.obj = Datacube(config=config).index


@cli.command()
@cli.argument('path')
@click.pass_obj
def create_product(index, path):
    datasets = find_datasets(Path(path))
    first_name = sorted(list(datasets))[0]
    sample_dataset = datasets[first_name]

    # display product def
    variables = dataset_to_variable_descriptions(sample_dataset)
    product_def = generate_product_defn(variables)
    print_dict(product_def)

    product = index.products.from_doc(product_def)
    index.products.add(product)


@cli.command()
@cli.argument('path')
@click.pass_obj
def index_data(index, path):
    datasets = find_datasets(Path(path))

    for name, dataset in datasets.items():
        dataset_doc = generate_dataset_doc(name, dataset)
        print_dict(dataset_doc)


def print_dict(doc):
    print(json.dumps(doc, indent=4, sort_keys=True, cls=NumpySafeEncoder))


def find_datasets(path: Path):
    """
    Find 1 day aggregate ocean colour products inside a dir

    They are split across multiple NetCDF files with a consistent prefix matching
    the pattern Ayyyymmdd.vv.aust.xxx.nc

    ``Ayyyymmdd.vv.aust.xxx.nc``, where

    - 'A' denotes MODIS/Aqua
    - 'yyyymmdd' is the GMT date of the mosaic
    - 'vv' is the SeaDAS processing version
    - 'aust' indicates a whole-of-Australia mosaic
    - ‘xxx’ indicates the product/variable name
    - 'nc' suffix is for netCDF4 format data files.

    """
    pattern = re.compile(r'(?P<sat>[AT])'
                         r'(?P<date>\d{8})\.'
                         r'(?P<version>[A-Z_\d]+)'
                         r'.aust.'
                         r'(?P<variable>[a-zA-z\d_]+)'
                         r'.nc')
    datasets = defaultdict(dict)
    for ncfile in path.iterdir():
        match = pattern.search(str(ncfile))
        if match:
            sat, date, version, variable = match.groups()
            dataset = sat + date + version
            datasets[dataset][variable] = ncfile

    return datasets


def dataset_to_variable_descriptions(dataset):
    variables = {}
    for _, ncfile in dataset.items():
        for varname, description in describe_variables(ncfile):
            variables[varname] = description
    return variables


def describe_variables(ncfile):
    """
    Each NetCDF file in this Ocean Colour set represents a single data variable.

    Pull all the useful information out of the variable for construction Product and Dataset docs.
    """
    nco = netCDF4.Dataset(ncfile)
    non_axis_variables = nco.get_variables_by_attributes(axis=lambda v: v is None)
    for vs in non_axis_variables:
        doc = {'name': vs.name,
               'description': vs.long_name,
               'dtype': str(vs.dtype),
               'nodata': vs._FillValue,
               'units': str(vs.units)}
        yield vs.name, doc


def generate_product_defn(variables):
    return {
        'name': 'modis_oc_1d',
        'metadata_type': 'eo',
        'metadata': {
            'product_type': 'modis_oc_1d',
            'platform': {'code': 'MODIS'},
            'version': 1,
            'coverage': 'aust'
        },
        'measurements': sorted(list(variables.values()), key=lambda d: d['name'])
    }


def generate_dataset_doc(dataset_name, dataset):
    """

    :param dataset: dictionary of varname: ncfile
    :return:
    """
    sample_ncfile = dataset['owtd']
    sample_ncfile_gdal = f'NETCDF:{sample_ncfile}:owtd'
    creation_time = datetime.fromtimestamp(sample_ncfile.stat().st_mtime)
    geo_ref_points, spatial_ref = get_grid_spatial_projection(sample_ncfile_gdal)

    start_time, end_time = name_to_date_range(dataset_name)
    # variables = dataset_to_variable_descriptions(dataset)

    unique_ds_uri = f'{sample_ncfile.as_uri()}#{creation_time}'

    doc = {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, unique_ds_uri)),
        'product_type': 'modis_oc_1d',
        'creation_dt': str(creation_time),
        'platform': {'code': 'MODIS'},
        'extent': {
            'from_dt': str(start_time),
            'to_dt': str(end_time),
            'coord': to_lat_long_extent(geo_ref_points),
        },
        'format': {'name': 'NetCDF'},
        'grid_spatial': {
            'projection': {
                'geo_ref_points': geo_ref_points,
                'spatial_reference': spatial_ref,
            }
        },
        'image': {
            'bands': {
                var_name: {
                    'path': f'NETCDF:{path}:{var_name}',
                    # 'layer': 1,
                } for var_name, path in dataset.items()
            }
        },

        'lineage': {'source_datasets': {}}
    }
    return doc


def name_to_date_range(name):
    date = name[1:9]
    start_time = datetime.strptime(date, '%Y%m%d')
    end_time = start_time + timedelta(days=1) - timedelta(microseconds=1)
    return start_time, end_time


def to_lat_long_extent(geo_ref_points):
    return {corner: {'lat': points['y'], 'lon': points['x']}
            for corner, points in geo_ref_points.items()}


def get_grid_spatial_projection(fname):
    with rasterio.open(fname, 'r') as img:
        left, bottom, right, top = img.bounds
        # spatial_reference = str(str(getattr(img, 'crs_wkt', None) or img.crs.wkt))
        spatial_reference = 'EPSG:4326'
        geo_ref_points = {
            'ul': {'x': left, 'y': top},
            'ur': {'x': right, 'y': top},
            'll': {'x': left, 'y': bottom},
            'lr': {'x': right, 'y': bottom},
        }
        return geo_ref_points, spatial_reference


def add_dataset(doc, uri, index, sources_policy):
    print(f"Indexing {uri}")
    resolver = Doc2Dataset(index)
    dataset, err = resolver(doc, uri)
    if err is not None:
        LOG.error("%s", err)
    try:
        index.datasets.add(dataset,
                           sources_policy=sources_policy)  # Source policy to be checked in sentinel 2 datase types
    except changes.DocumentMismatchError as e:
        index.datasets.update(dataset, {tuple(): changes.allow_any})
    except Exception as e:
        logging.error("Unhandled exception %s", e)

    return uri


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
