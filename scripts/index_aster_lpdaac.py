#!/usr/bin/env python
"""
This program allows indexing the Australia region ASTER (Advanced Spaceborne Thermal
Emission and Reflection Radiometer)  L1T data stored on the NCI into an ODC Database.

ASTER data consists of visible and near infrared (VNIR) frequencies
with three bands at 15-meter resolution, short-wave infrared (SWIR)
frequencies with six bands at 30-meter resolution, and  thermal infrared (TIR)
wavelength with five bands at 90-meter resolution.

Further details of AST_L1T data is available from
https://lpdaac.usgs.gov/dataset_discovery/aster/aster_products_table/ast_l1t_v003

The ASTER L1T data product is derived from ASTER Level 1A data that has been
geometrically corrected and reprojected to a north-up Universal Transverse Mercator (UTM)
projection.
(Please see: https://lpdaac.usgs.gov/sites/default/files/public/elearning/ASTER_L1T_Tutorial.html)

Further, depending on whether the following modes are enabled, dataset may present
different bands:
  ASTEROBSERVATIONMODE.1=VNIR1, ON/OFF
  ASTEROBSERVATIONMODE.2=VNIR2, ON/OFF
  ASTEROBSERVATIONMODE.3=SWIR, ON/OFF
  ASTEROBSERVATIONMODE.4=TIR, ON/OFF

Regarding `SWIR` bands please note the following advice from
https://asterweb.jpl.nasa.gov/swir-alert.asp

::
    ASTER SWIR detectors are no longer functioning due to anomalously high SWIR detector
    temperatures. ASTER SWIR data acquired since April 2008 are not useable, and
    show saturation of values and severe striping. All attempts to bring the SWIR bands
    back to life have failed, and no further action is envisioned. -- January 12, 2009
::

It runs in two modes, one to create the product definition in the database,
 and the second to record
dataset details. Both modes need to be pointed at a directory of ASTER_L1T data
stored in hdf format.

The data is stored in sets of hdf files
in `/g/data/v10/ASTER_AU/`.

The script  can be run with `create-product`, `create-vrt`
or `index-data` parameter mode, and an output directory of hdf files.
 It reads the hdf files to create the Product/VRT/Dataset
definitions, and write the datasets directly into an ODC database.
It doesn't write out intermediate YAML files.

The ODC Index datasets points to to the corresponding VRT files in order to access
raster measurement data. The VRT file in turn points to the original `.hdf` file
through `absolute path names` (Relative path names are not working at the moment,
and it is advised that the VRT files must be generated for the final resident
location of `.hdf` files.

Each VRT file specify consistent set of bands from ASTER as a single product.
For example, `vnir` sensors correspond to `aster_l1t_vnir` product, `tir`
sensors correspond to `aster_l1t_tir` product, and `swir` sensors correspond
to `aster_l1t_swir` product. The corresponding definitions of these product
names and corresponding bands (with band names as identified in the original
`hdf` file) are defined in the constant `PRODUCTS` in this script.

It attempts to create stable UUIDs for the generated Datasets,
based on the file path and modification time of the underlying HDF file Data
as well as product name. Use following commands to create a product definition
and add it to datacube, create a corresponding VRT file, and create a
dataset definition and add it to datacube.


::

    ./index_nci_aster_lpdaac.py create-product
                        --product aster_l1t_vnir /g/data/v10/ASTER_AU/2018.01.01
    ./index_nci_aster_lpdaac.py create-vrt
                        --product aster_l1t_vnir /g/data/v10/ASTER_AU/2018.01.01
    ./index_nci_aster_lpdaac.py index-data
                        --product aster_l1t_vnir /g/data/v10/ASTER_AU/2018.01.01


::

    psql -h agdcdev-db.nci.org.au
    CREATE DATABASE aster_lpdaac WITH OWNER agdc_admin;
    GRANT TEMPORARY, CONNECT ON DATABASE aster_lpdaac to public;

aster_lpdaac.conf::

    [datacube]
    db_hostname: agdcdev-db.nci.org.au
    db_port: 6432
    db_database: aster_lpdaac

::

    datacube --config aster_lpdaac.conf system init

::

    for i in /g/data/v10/ASTER_AU/*; do
        ./index_nci_aster_lpdaac.py --config aster_lpdacc.conf index-data
                                    --product aster_l1t_vnir $i
    done

"""
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
import functools
import yaml

import click
import numpy as np
from osgeo import gdal, osr
import rasterio

from datacube import Datacube
from datacube.index.hl import Doc2Dataset


LOG = logging.getLogger(__name__)

PRODUCTS = {'aster_l1t_vnir': {'ImageData2', 'ImageData1', 'ImageData3N'},
            'aster_l1t_swir': {'ImageData4', 'ImageData5', 'ImageData6', 'ImageData7', 'ImageData8', 'ImageData9'},
            'aster_l1t_tir': {'ImageData10', 'ImageData11', 'ImageData12', 'ImageData13', 'ImageData14'}}

EXTRA_METADATA_PREFIXES = {
    'aster_l1t_vnir': {'include_only': {'ASTER', 'CORRECT', 'EAST', 'FLY', 'GAIN', 'INPUT', 'LOWER', 'MAP',
                                        'NORTH', 'NUMBERGCP', 'ORBIT', 'POINT', 'QAPERCENT', 'RECURRENT', 'SCENE',
                                        'SIZE', 'SOLAR', 'SOUTH', 'UPPER', 'UTMZONENUMBER', 'WEST'}},
    'aster_l1t_swir': {'include_only': {'ASTER', 'CORRECT', 'EAST', 'FLY', 'GAIN', 'INPUT', 'LOWER', 'MAP',
                                        'NORTH', 'NUMBERGCP', 'ORBIT', 'POINT', 'QAPERCENT', 'RECURRENT', 'SCENE',
                                        'SIZE', 'SOLAR', 'SOUTH', 'UPPER', 'UTMZONENUMBER', 'WEST'}},
    'aster_l1t_tir': {'exclude': {'BAND', 'CALENDAR', 'COARSE', 'FUTURE', 'GEO', 'HDF', 'IDENT', 'IMAGE',
                                  'PGE', 'PROCESSED', 'PROCESSING', 'RADIO', 'RECEIVING', 'REPROCESSING', 'SOURCE',
                                  'TIME', 'UTMZONECODE'}}
}


@click.group(help=__doc__)
@click.option('--config', '-c', help="Pass the configuration file to access the database",
              type=click.Path(exists=True))
@click.pass_context
def cli(ctx, config):
    """ Used to pass the datacube index to functions via click."""
    ctx.obj = Datacube(config=config).index


@cli.command()
@click.argument('path')
@click.option('--product', help='Which ASTER product? vnir, swir, or tir')
def create_vrt(path, product):

    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)

    for file_path in file_paths:
        bands = selected_bands(file_path, product)
        if bands:
            vrt = generate_vrt(file_path, bands)
            with open(vrt_file_path(file_path, product), 'w') as fd:
                fd.write(vrt)
        else:
            logging.error("File does not have bands of this product: %s", file_path)


@cli.command()
@click.argument('path')
@click.option('--product', help='Which ASTER product? vnir, swir, or tir')
@click.pass_obj
def show(index, path, product):

    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)

    _ = Doc2Dataset(index)
    for file_path in file_paths:
        bands = selected_bands(file_path, product)

        if bands:
            doc = generate_lpdaac_doc(file_path, product)
            print_dict(doc)
        else:
            logging.error("File does not have bands of this product: %s", file_path)


@cli.command()
@click.argument('path')
@click.option('--product', help='Which ASTER product? vnir, swir, or tir')
@click.pass_obj
def create_product(index, path, product):

    file_paths = find_lpdaac_file_paths(Path(path))

    # Find a file which has the specified bands of this product
    file_path = None
    for file_path_ in file_paths:
        bands_ = selected_bands(file_path_, product)
        if len(bands_) == len(PRODUCTS[product]):
            file_path = file_path_
            break

    if file_path:
        measurements = raster_to_measurements(file_path, product)
        for measure in measurements:
            measure.pop('path')  # This is not needed here
        print_dict(measurements)
        product_def = generate_lpdaac_defn(measurements, product)
        print_dict(product_def)

        print(index)
        product = index.products.from_doc(product_def)
        print(product)
        indexed_product = index.products.add(product)
        print(indexed_product)
    else:
        logging.error("No file found having the specified bands of this product: %s", product)


@cli.command()
@click.argument('path')
@click.option('--product', help='Which ASTER product? vnir, swir, or tir')
@click.pass_obj
def index_data(index, path, product):
    file_paths = find_lpdaac_file_paths(Path(path))
    print(file_paths)

    resolver = Doc2Dataset(index)
    for file_path in file_paths:

        bands = selected_bands(file_path, product)
        if bands:
            vrt_path = vrt_file_path(file_path, product)

            if vrt_path.exists():

                doc = generate_lpdaac_doc(file_path, product)
                print_dict(doc)
                dataset, err = resolver(doc, vrt_path.as_uri())

                print(dataset)
                if err is not None:
                    logging.error("%s", err)
                try:
                    index.datasets.add(dataset)
                except Exception as e:
                    logging.error("Couldn't index %s", file_path)
                    logging.exception("Exception", e)
                else:
                    with open(yaml_file_path(file_path, product), 'w') as yaml_file:
                        yaml.safe_dump(doc, yaml_file)

            else:
                logging.error("VRT file not found: %s", vrt_path)
        else:
            logging.error("File does not have bands of this product: %s", file_path)


def vrt_file_path(file_path, product):
    return file_path.with_name(f'{file_path.stem}_{product}.vrt')


def yaml_file_path(file_path, product):
    return file_path.with_name(f'{file_path.stem}_{product}.yaml')


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
        if afile.suffix == '.hdf' and afile.stem[:7] == 'AST_L1T':
            file_paths.append(afile)
    return file_paths


def raster_to_measurements(file_path, product):

    bands = selected_bands(file_path, product)

    measurements = []
    for index, band in enumerate(bands):
        measure = dict(name=str(index + 1))
        measure['path'] = vrt_file_path(file_path, product).name

        with rasterio.open(band) as band_:
            measure['dtype'] = str(band_.dtypes[0])
            measure['nodata'] = band_.nodatavals[0] or 0
            measure['units'] = str(band_.units[0] or 1)
            measure['aliases'] = [band.split(':')[-1]]
        measurements.append(measure)
    return measurements


@functools.lru_cache(maxsize=None)
def selected_bands(file_path, product):

    band_suffixes = PRODUCTS[product]

    ds = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    sub_datasets = ds.GetSubDatasets()
    # Check the last field of the band name: something like 'ImageDataXX'

    available_bands_of_product = [band[0] for band in sub_datasets if band[0].split(':')[-1] in band_suffixes]

    assert len(available_bands_of_product) == len(band_suffixes)

    available_bands_of_product.sort(key=lambda x: x.split(':')[-1])

    return available_bands_of_product


def generate_lpdaac_defn(measurements, product):
    """
    Generate the product def for the product.
    """
    return {
        'name': product,
        'metadata_type': 'eo',
        'metadata': {
            'product_type': product,
            'platform': {'code': 'ASTER'},
            'version': 1,
            'coverage': 'aust'
        },
        'description': 'ASTER L1T - Precision Terrain Corrected Registered At-Sensor Radiance data',
        'measurements': measurements
    }


def generate_lpdaac_doc(file_path, product):

    modification_time = file_path.stat().st_mtime

    unique_ds_uri = f'{file_path.as_uri()}#{modification_time}#{product}'

    left, bottom, right, top = compute_extents(file_path)
    spatial_ref = infer_aster_srs(file_path)
    geo_ref_points = {
        'ul': {'x': left, 'y': top},
        'ur': {'x': right, 'y': top},
        'll': {'x': left, 'y': bottom},
        'lr': {'x': right, 'y': bottom},
    }

    acquisition_time = get_acquisition_time(file_path)
    measurements = raster_to_measurements(file_path, product)

    the_format = 'VRT'

    doc = {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, unique_ds_uri)),
        'product_type': product,
        'creation_dt': str(datetime.fromtimestamp(modification_time)),
        'platform': {'code': 'ASTER'},
        'extent': {
            'from_dt': str(acquisition_time),
            'to_dt': str(acquisition_time),
            'coord': geo_ref_points
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
                    'path': measure['path'],
                    'layer': str(index + 1)
                } for index, measure in enumerate(measurements)
            }
        },
        'version': 1,
        'coverage': 'aust',
        'lineage': {'source_datasets': {}},
        'further_info': filter_metadata(file_path, product)
    }
    return doc


def infer_aster_srs(file_path: Path):
    """
    Compute SRS based on metadata (UTMZONENUMBER and NORTHBOUNDINGCOORDINATE) in the file and
    generic osr.SpatialReference data.
    Reference:
    https://git.earthdata.nasa.gov/projects/LPDUR/repos/aster-l1t/raw/ASTERL1T_hdf2tif.py?at=refs%2Fheads%2Fmaster
    """

    ds = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    meta = ds.GetMetadata()

    # Define UL, LR, UTM zone
    utm = np.int(meta['UTMZONENUMBER'])
    n_s = np.float(meta['NORTHBOUNDINGCOORDINATE'])

    # Create UTM zone code numbers
    utm_n = [i + 32600 for i in range(60)]
    utm_s = [i + 32700 for i in range(60)]

    # Define UTM zone based on North or South
    if n_s < 0:
        utm_zone = utm_s[utm]
    else:
        utm_zone = utm_n[utm]

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(utm_zone)

    return srs.ExportToWkt()


def generate_vrt(file_path: Path, bands):
    """
    Generate a VRT file for a given file
    The following tags did not show visual impact on raster bands when rendering:
        1. Top level GeoTransform
    """

    assert bands

    x_size, y_size = get_raster_sizes(bands)
    geo_transform = compute_geo_transform(file_path, bands)

    return """\
    <VRTDataset rasterXSize="{x}" rasterYSize="{y}">
        <SRS>{srs}</SRS>
        <GeoTransform>{geo}</GeoTransform>
        {raster_bands}
    </VRTDataset>
    """.format(x=x_size, y=y_size, srs=infer_aster_srs(file_path),
               geo=', '.join(('%1.16e' % v for v in geo_transform)),
               raster_bands=get_raster_bands_vrt(bands))


def get_raster_bands_vrt(bands):
    """
    Compute the <VRTRasterBand> tags for each band ang return them as a single string

    :param bands: GDAL SubDatasets
    """

    raster_band_template = """\
    <VRTRasterBand dataType="{dtype}" band="{number}">
        <NoDataValue>{nodata}</NoDataValue>
        <ComplexSource>
            <SourceFilename relativeToVRT="0">{band_name}</SourceFilename>
        </ComplexSource>
    </VRTRasterBand>
    """

    raster_bands = ''
    for index, band in enumerate(bands):
        sdt = gdal.Open(band, gdal.GA_ReadOnly)
        data_type = gdal.GetDataTypeName(sdt.GetRasterBand(1).DataType)
        raster_bands += raster_band_template.format(dtype=data_type, number=str(index + 1),
                                                    nodata=0,
                                                    band_name=band)
    return raster_bands


def get_raster_sizes(bands):
    """
    Raster sizes of different bands are different. So compute the max of x axis
    and max of y axis

    :param bands: GDAL SubDataset names
    """

    x_size = []
    y_size = []
    for band in bands:
        sdt = gdal.Open(band, gdal.GA_ReadOnly)
        x_size.append(sdt.RasterXSize)
        y_size.append(sdt.RasterYSize)
    return max(x_size), max(y_size)


def get_acquisition_time(file_path):

    dt = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    meta = dt.GetMetadata()
    date_string = meta['CALENDARDATE']

    time_ = meta['TIMEOFDAY']

    return datetime(year=int(date_string[:4]), month=int(date_string[4:6]), day=int(date_string[6:8]),
                    hour=int(time_[:2]), minute=int(time_[2:4]), second=int(time_[4:6]),
                    microsecond=int(time_[6:12]), tzinfo=timezone.utc)


def compute_geo_transform(file_path, bands):
    """
    Compute the geo transform for the given bands. If the geo transform is not same
    for all the given bands an assert error is raised.
    """

    # pylint: disable=round-builtin

    dt = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    meta = dt.GetMetadata()

    # Define UL, LR, UTM zone
    ul = [np.float(x) for x in meta['UPPERLEFTM'].split(', ')]
    lr = [np.float(x) for x in meta['LOWERRIGHTM'].split(', ')]
    n_s = np.float(meta['NORTHBOUNDINGCOORDINATE'])

    # Define extent and provide offset for UTM South zones
    if n_s < 0:
        ul_y = ul[0] + 10000000
        ul_x = ul[1]

        lr_y = lr[0] + 10000000
        lr_x = lr[1]

    # Define extent for UTM North zones
    else:
        ul_y = ul[0]
        ul_x = ul[1]

        lr_y = lr[0]
        lr_x = lr[1]

    # We want all the bands to be consistent in terms of data type,
    # raster number of columns and rows
    band_info = dict()
    band_info['ncol'] = set()
    band_info['nrow'] = set()
    band_info['data_type'] = set()
    for band in bands:
        band_ds = gdal.Open(band, gdal.GA_ReadOnly)
        data_type = gdal.GetDataTypeName(band_ds.GetRasterBand(1).DataType)
        if data_type == 'Byte':
            band_data = band_ds.ReadAsArray().astype(np.byte)
        elif data_type == 'UInt16':
            band_data = band_ds.ReadAsArray().astype(np.uint16)
        else:
            raise ValueError('Unexpected band type')

        # Query raster dimensions
        ncol, nrow = band_data.shape

        band_info['data_type'].add(data_type)
        band_info['ncol'].add(ncol)
        band_info['nrow'].add(nrow)

    assert len(band_info['data_type']) == 1 and len(band_info['ncol']) == 1 and len(band_info['nrow']) == 1

    # Compute resolutions
    y_res = -1 * round((max(ul_y, lr_y) - min(ul_y, lr_y)) / band_info['ncol'].pop())
    x_res = round((max(ul_x, lr_x) - min(ul_x, lr_x)) / band_info['nrow'].pop())

    # Define UL x and y coordinates based on spatial resolution
    ul_yy = ul_y - (y_res / 2)
    ul_xx = ul_x - (x_res / 2)

    return ul_xx, x_res, 0., ul_yy, 0., y_res


def compute_extents(file_path):
    """
    Compute the union of extents of individual raster bands.
    https://git.earthdata.nasa.gov/projects/LPDUR/repos/aster-l1t/raw/ASTERL1T_hdf2tif.py?at=refs%2Fheads%2Fmaster
    """
    dt = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    meta = dt.GetMetadata()

    # Define LL, UR
    ll = [np.float(x) for x in meta['LOWERLEFTM'].split(', ')]
    ur = [np.float(x) for x in meta['UPPERRIGHTM'].split(', ')]
    n_s = np.float(meta['NORTHBOUNDINGCOORDINATE'])
    # Define extent and provide offset for UTM South zones
    if n_s < 0:
        ll_y = ll[0] + 10000000
        ll_x = ll[1]

        ur_y = ur[0] + 10000000
        ur_x = ur[1]

    # Define extent for UTM North zones
    else:
        ll_y = ll[0]
        ll_x = ll[1]

        ur_y = ur[0]
        ur_x = ur[1]

    # Do we need to offset pixel center by half of pixel resolution as in the above reference?
    # Note: pixel resolution vary per band

    return ll_x, ll_y, ur_x, ur_y


def filter_metadata(file_path, product):
    """
    Filter the metadata dictionary based on what is to include or exclude defined by
    the global EXTRA_METADATA_PREFIXES
    """

    dt = gdal.Open(str(file_path), gdal.GA_ReadOnly)
    meta = dt.GetMetadata()
    items = set()
    if EXTRA_METADATA_PREFIXES[product].get('include_only'):
        for prefix in EXTRA_METADATA_PREFIXES[product]['include_only']:
            items.update({meta_item for meta_item in meta if meta_item.startswith(prefix)})
    elif EXTRA_METADATA_PREFIXES[product].get('exclude'):
        for prefix in EXTRA_METADATA_PREFIXES[product]['exclude']:
            items.update({meta_item for meta_item in meta})
            items.difference({meta_item for meta_item in meta if meta_item.startswith(prefix)})
    return {item: meta[item] for item in items}


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
