#!/usr/bin/env python

import datacube
import xarray as xr
import yaml
import time
import click
from os.path import abspath


def open_unindexed_nc(fname, product, dc=None):
    if dc is None:
        dc = datacube.Datacube(app='app')

    if isinstance(product, str):
        product = dc.index.products.get_by_name(product)
        if product is None:
            print('Failed to load product: {}'.format(product))
            return None, None, None, None

    uris = ['file://' + abspath(fname)]

    def mk_dataset(yaml_string):
        return datacube.model.Dataset(product, yaml.load(yaml_string, Loader=yaml.CSafeLoader), uris=uris)

    with xr.open_dataset(fname) as f:
        datasets = [mk_dataset(f.dataset.values[i].decode('utf-8')) for i in range(f.dataset.shape[0])]

    data_group = dc.group_datasets(datasets, datacube.api.query.query_group_by())

    geom = datacube.api.core.get_bounds(datasets, product.grid_spec.crs)
    geobox = datacube.utils.geometry.GeoBox.from_geopolygon(geom, product.grid_spec.resolution)

    return data_group, geobox, product, dc


def run_test(fname, product_name):
    print('Processing file: {}\n product: {}'.format(fname, product_name))
    print('Preparing dataset for loading (cold)')
    t0 = time.time()
    data_group, geobox, product, dc = open_unindexed_nc(fname, product_name)
    t_prep = time.time() - t0
    if data_group is None:
        return

    print('Took {} secs'.format(t_prep))  # According to profile it's mostly yaml parsing

    if False:
        print('Preparing dataset for loading (warm)')
        t0 = time.time()
        data_group, geobox, product, dc = open_unindexed_nc(fname, product_name)
        t_prep = time.time() - t0
        print('Took {} secs'.format(t_prep))

    for k in product.measurements.keys():
        t0 = time.time()
        x = dc.load_data(data_group, geobox, [product.measurements[k]])
        dt = time.time() - t0
        nslices = x.time.shape[0]
        W, H = x.x.shape[0], x.y.shape[0]
        print(' Loaded band: {} ({}x{}, x{} slices), took {} secs'.format(k, W, H, nslices, dt))


@click.command()
@click.option('--product', type=str, default='ls7_nbart_albers')
@click.argument('files', nargs=-1)
def main(product, files):
    ''' Benchmark loading of data from stacked NetCDF
    '''

    base = '/g/data/uc0/rs0_dev/20170223-stack_and_ncml/LS7_ETM_NBART/15_-40/'
    file_name = 'LS7_ETM_NBART_3577_15_-40_1999_v1490223057.nc'

    if len(files) == 0:
        files = [base + file_name]

    for fname in files:
        run_test(fname, product)


if __name__ == '__main__':
    main()
