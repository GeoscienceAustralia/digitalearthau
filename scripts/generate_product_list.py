import csv
import os
import re

from datetime import datetime
from urllib.parse import urlencode
from pathlib import Path

import datacube
from datacube.model import Range

THREDDS_PRODUCT_LIST = [
    'ls5_nbar_albers',
    'ls7_nbar_albers',
    'ls8_nbar_albers',
    'ls5_nbart_albers',
    'ls7_nbart_albers',
    'ls8_nbart_albers',
    'ls5_pq_albers',
    'ls7_pq_albers',
    'ls8_pq_albers'
]

# 1986 is the start of our landsat 5 nbar collection, run until the present
YEAR_RANGE = list(range(1986, datetime.now().year))
OUTPUT_DIR = Path('./files')


# Fields
class ObservationDateField:
    NAME = 'observation_date'

    @classmethod
    def get_value(cls, dss_record):
        """get_value: Returns the observation date for the record

        :param dss_record: A dataset record
        """
        try:
            return dss_record.time.begin.isoformat()
        except AttributeError:
            return ''


class CreationDateField:
    NAME = 'creation_date'

    @classmethod
    def get_value(cls, dss_record):
        """get_value: Returns the creation date of the record

        :param dss_record: A dataset record
        """
        try:
            return dss_record.metadata.creation_dt
        except (KeyError, AttributeError):
            return ''


class SpatialReferenceField:
    NAME = 'spatial_reference'

    @classmethod
    def get_value(cls, dss_record):
        """get_value: Returns the spatial reference for the dataset record

        :param dss_record: A dataset record
        """
        try:
            return dss_record.crs
        except AttributeError:
            return ''


class CoordinateFieldFactory:

    COORDINATE_MAP = {
        'eastings': 0,
        'northings': 1
    }

    CACHE = {}

    # file:///{ignored}/{ignored}/{ignored}/{ignored}/{ignored}/{ignored}/-14_-12/{ignored}
    REGEX_PATTERN = re.compile('file://(?:/[^/]*){6}/([-0-9]+)_([-0-9]+)')

    @classmethod
    def get_coordinate_field(cls, coordinate):
        factory_cls = cls

        class _CoordinateClass:
            NAME = coordinate

            @classmethod
            def get_value(cls, dss_record):
                return factory_cls.get_coordinate_value(dss_record, coordinate)

        return _CoordinateClass

    @classmethod
    def get_coordinate_value(cls, dss_record, coordinate):
        coordinate_idx = cls.COORDINATE_MAP[coordinate]
        if cls.CACHE.get('coordinate_id') == dss_record.id:
            return cls.CACHE.get('coordinate_value')[coordinate_idx]

        coordinates = cls.REGEX_PATTERN.match(dss_record.local_uri).groups()

        cls.CACHE['coordinate_id'] = dss_record.id
        cls.CACHE['coordinate_value'] = coordinates

        return cls.CACHE.get('coordinate_value')[coordinate_idx]


class GeoTIFFFieldFactory:
    """GeoTIFFFieldFactory: Factory class for converting dss_records to wcs geotiff urls"""
    NCI_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
    THREDDS_SERVER = 'http://dapds00.nci.org.au/thredds/wcs'
    CACHE = {}

    @classmethod
    def get_band_field(cls, band):
        """get_band_field: Returns a field class that will return corresponding band url

        :param band: The band of the band to return the field for
        """
        factory_cls = cls

        class _BandClass:
            NAME = band

            @classmethod
            def get_value(cls, dss_record):
                params = {
                    'service': 'WCS',
                    'version': '1.0.0',
                    'request': 'GetCoverage',
                    'format': 'GeoTIFF',
                    'coverage': band,
                    'time': factory_cls.get_begin_time(dss_record),
                    'bbox': factory_cls.get_bounding_box(dss_record),
                    band: '100.0'
                }
                return "{band}: {server}/{file_path}?{params}".format(
                    band=band,
                    server=factory_cls.THREDDS_SERVER,
                    file_path=factory_cls.get_file_path(dss_record),
                    params=urlencode(params)
                )

        return _BandClass

    @classmethod
    def get_begin_time(cls, dss_record):
        """get_begin_time: Returns the beginning time from dds record for geotiff query
        Caching the last requested result

        :param dss_record: Record being queried
        """
        if cls.CACHE.get('begin_time_id') == dss_record.id:
            return cls.CACHE.get('begin_time_value')

        cls.CACHE['begin_time_id'] = dss_record.id
        cls.CACHE['begin_time_value'] = dss_record.time.begin.strftime(cls.NCI_DATE_FORMAT)

        return cls.CACHE.get('begin_time_value')

    @classmethod
    def get_file_path(cls, dss_record):
        """get_file_path: Returns the file path for the url conversion, caching the last requested result

        :param dss_record: Record being queried
        """
        if cls.CACHE.get('file_path_id') == dss_record.id:
            return cls.CACHE.get('file_path_value')

        cls.CACHE['file_path_id'] = dss_record.id
        cls.CACHE['file_path_value'] = dss_record.local_uri.replace('file:///g/data/', '')

        return cls.CACHE.get('file_path_value')

    @classmethod
    def get_bounding_box(cls, dss_record):
        """get_bounding_box: Returns the bounding box for the url conversion, caching the last requested result

        :param dss_record: Record being queried
        """
        if cls.CACHE.get('bounding_box_id') == dss_record.id:
            return cls.CACHE.get('bounding_box_value')

        cls.CACHE['bounding_box_value'] = ",".join((
            str(dss_record.metadata.lon.begin),
            str(dss_record.metadata.lat.end),
            str(dss_record.metadata.lon.begin),
            str(dss_record.metadata.lat.end)
        ))

        cls.CACHE['bounding_box_id'] = dss_record.id
        return cls.CACHE.get('bounding_box_value')


class DCReader:
    """DCReader: Iterates over products and years to provide product list dataset"""

    def __init__(self, dc, product_list, year_range):
        self.product_list = product_list
        self.year_range = year_range
        self.datacube = dc
        self.curr_idx = None

    def get_current_bands(self):
        return (
            self.datacube.index.products
            .get_by_name(self.product_list[self.curr_idx['product']])
            .measurements.keys()
        )

    def __iter__(self):
        self.curr_idx = {'product': 0, 'year': -1}
        return self

    def __next__(self):
        self.curr_idx['year'] += 1

        if self.curr_idx['year'] >= len(self.year_range):
            self.curr_idx['year'] = 0
            self.curr_idx['product'] += 1

        if self.curr_idx['product'] >= len(self.product_list):
            raise StopIteration

        return (
            self.product_list[self.curr_idx['product']],
            self.year_range[self.curr_idx['year']],
            self.datacube.index.datasets.search(
                product=self.product_list[self.curr_idx['product']],
                time=Range(
                    datetime(self.year_range[self.curr_idx['year']], 1, 1),
                    datetime(self.year_range[self.curr_idx['year']] + 1, 1, 1)
                )
            )
        )


class CSVWriter:
    """CSVWriter: Object that configures and controls writing datasets to a csv file"""

    LIST_DELIM = ' | '

    def __init__(self):
        self.headers = None
        self.file_path = None
        self.fdesc = None
        self.writer = None

    def configure(self, file_path, headers):
        self.file_path = file_path
        self.headers = headers

    def open(self):
        """open: calls enter and writes out the headers"""
        self.__enter__()
        self.write(self.headers)

    def close(self, valid_data):
        self.__exit__()

        if not valid_data:
            self.file_path.unlink()

    def create_outdir(self):
        """create_outdir: Creates configured output directory"""
        outdir = Path(*self.file_path.absolute().parts[:-1])
        outdir.mkdir(parents=True, exist_ok=True)

    def __enter__(self):
        """__enter__: Creates output directory and opens csv file"""
        self.create_outdir()
        self.fdesc = self.file_path.open(mode='w')
        self.writer = csv.writer(self.fdesc)
        return self.writer

    def write(self, data):
        """write writes the data to the configured csv file

        :param data: a list containing the records to write out
        """
        for i, _ in enumerate(data):
            if isinstance(data[i], str):
                continue
            elif hasattr(data[i], '__iter__'):
                data[i] = self.LIST_DELIM.join([str(txt) for txt in data[i]])
            else:
                data[i] = str(data[i])

        self.writer.writerow(data)

    def __exit__(self, *exc_details):
        """__exit__: Closing the open file descriptor

        :param *exc_details: Inbuilt args
        """
        if self.fdesc:
            self.fdesc.close()
        self.fdesc = None


def process_product_list(product_generator, product_writer):
    """process_product_list
    Iterates over the list of products and writes to the configured output.

    :param product_generator: (object) An iterator over a list of products
    :param product_writer: (object) A writer that implements configure, open, write, close.
    """

    data_row = None
    for product, year, dataset in product_generator:

        fields = [
            ObservationDateField,
            CreationDateField,
            SpatialReferenceField,
            CoordinateFieldFactory.get_coordinate_field('eastings'),
            CoordinateFieldFactory.get_coordinate_field('northings')
        ]

        # Add a field for each band
        for band in product_generator.get_current_bands():
            fields.append(GeoTIFFFieldFactory.get_band_field(band))

        headers = list(map(lambda field: field.NAME, fields))

        product_writer.configure(
            OUTPUT_DIR / product / "{}.csv".format(year),  # Pathlib path concatenation
            headers
        )
        product_writer.open()

        for data_row in dataset:
            product_writer.write(
                list(map(lambda field: field.get_value(data_row), fields))
            )

        product_writer.close(valid_data=(data_row is not None))


def main(product_list=None, year_range=None):
    """
    Reads product list from the database and creates a directory of csv files.
    """
    product_generator = DCReader(
        datacube.Datacube(),
        (product_list or THREDDS_PRODUCT_LIST),
        (year_range or YEAR_RANGE)
    )

    product_writer = CSVWriter()

    process_product_list(product_generator, product_writer)


if __name__ == '__main__':
    import argparse

    PARSER = argparse.ArgumentParser(description='Generates a product list.')
    PARSER.add_argument('--year', type=int, nargs="+", dest="year_range", help='Year to process')
    PARSER.add_argument('--product', nargs="+", dest="product_list", help='Product to process')

    ARGS = vars(PARSER.parse_args())
    main(product_list=ARGS.get('product_list'), year_range=ARGS.get('year_range'))
