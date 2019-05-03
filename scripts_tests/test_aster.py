import lzma
from pathlib import Path

import pytest

from datacube.index.hl import Doc2Dataset
from scripts.index_aster_lpdaac import generate_lpdaac_defn, generate_lpdaac_doc, generate_vrt
from scripts.index_aster_lpdaac import selected_bands, raster_to_measurements, vrt_file_path
from scripts.index_aster_lpdaac import PRODUCTS

pytest_plugins = "digitalearthau.testing.plugin"

SCRIPTS_TEST_DATA = Path(__file__).parent / 'data'


def uncompress_xz(in_file, dest_file):
    """
    Uncompress the lzma compressed file
    """
    with lzma.open(in_file, 'rb') as fin, open(dest_file, 'wb') as fout:
        fout.write(fin.read())


@pytest.fixture
def aster_files(tmp_path):
    """
    Return the locations of aster HDF files
    """
    dest_file1 = tmp_path / 'AST_L1T_00312102017022934_20171211115854_25347.hdf'
    uncompress_xz(SCRIPTS_TEST_DATA / 'aster' / '2017.12.10' / 'shrunk.hdf.xz',
                  dest_file1)

    dest_file2 = tmp_path / 'AST_L1T_00312272007012132_20150522121457_113468.hdf'
    uncompress_xz(SCRIPTS_TEST_DATA / 'aster' / '2007.12.27' / 'shrunk.hdf.xz',
                  dest_file2)

    return dest_file1, dest_file2


def products_present(aster_file_path):
    """
    Return products present in a given aster file
    """
    products = []
    for product in PRODUCTS:
        try:
            # See if band extraction fails for this product
            _ = selected_bands(aster_file_path, product)

            products.append(product)
        except AssertionError:
            pass
    return products


def test_product_defs(aster_files):
    """
    Test product definitions generated for given files
    """
    for aster_file in aster_files:
        for product in products_present(aster_file):
            measurements = raster_to_measurements(aster_file, product)
            for measure in measurements:
                measure.pop('path')  # This is not needed here
            product_def = generate_lpdaac_defn(measurements, product)

            assert product_def['metadata']['product_type'] == product
            # Check all expected band names ['1', '2', '3', etc]
            num_bands = len(PRODUCTS[product]['bands'])
            assert all([a == b for a, b in zip([str(band_num)
                                                for band_num in range(1, num_bands + 1)],
                                               [m['name'] for m in product_def['measurements']])])


def test_vrt_generation(aster_files):
    """
    Test generated VRT strings for given files
    """
    import xml.etree.ElementTree as ET
    import xmlschema

    for aster_file in aster_files:
        for product in products_present(aster_file):
            bands = selected_bands(aster_file, product)
            vrt = generate_vrt(aster_file, bands)

            # Is it valid VRT schema
            xsd = xmlschema.XMLSchema(str(SCRIPTS_TEST_DATA / 'aster/vrt_schema.xsd'))
            xsd.validate(vrt)

            tree = ET.fromstring(vrt)

            assert len(tree.findall('VRTRasterBand')) == len(PRODUCTS[product]['bands'])
            sources = tree.findall('SourceFilename')
            for source in sources:
                parts = source.text.split(':')
                # We want the source path name to be absolute
                assert aster_file == Path(parts[2])
                assert parts[4] in PRODUCTS[product]['bands']


def test_dataset_doc(aster_files):
    """
    Test dataset docs generated for given files.
    """
    for aster_file in aster_files:
        for product in products_present(aster_file):
            doc = generate_lpdaac_doc(aster_file, product)
            assert doc['grid_spatial']['projection']['spatial_reference']
            assert len(doc['image']['bands']) == len(PRODUCTS[product]['bands'])


def test_dataset_indexing(dea_index, aster_files):
    """
    Test datacube indexing for each product for the given files
    """

    for aster_file in aster_files:
        for product in products_present(aster_file):
            vrt_path = vrt_file_path(aster_file, product)
            measurements = raster_to_measurements(aster_file, product)
            for measure in measurements:
                measure.pop('path')  # This is not needed here
            product_def = generate_lpdaac_defn(measurements, product)
            product_ = dea_index.products.from_doc(product_def)
            indexed_product = dea_index.products.add(product_)

            assert indexed_product

            doc = generate_lpdaac_doc(aster_file, product)
            resolver = Doc2Dataset(dea_index)
            dataset, _ = resolver(doc, vrt_path.as_uri())
            print('the dataset to be indexed: ', dataset)
            dea_index.datasets.add(dataset)
