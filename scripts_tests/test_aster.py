import lzma
from pathlib import Path

import pytest

from datacube.index.hl import Doc2Dataset
from scripts.index_aster_lpdaac import generate_lpdaac_defn, generate_lpdaac_doc, generate_vrt, selected_bands
from scripts.index_aster_lpdaac import raster_to_measurements, vrt_file_path

pytest_plugins = "digitalearthau.testing.plugin"

SCRIPTS_TEST_DATA = Path(__file__).parent / 'data'

PRODUCTS = {'aster_l1t_vnir': {'ImageData2', 'ImageData1', 'ImageData3N'},
            'aster_l1t_tir': {'ImageData10', 'ImageData11', 'ImageData12', 'ImageData13', 'ImageData14'}}

EXTRA_METADATA_PREFIXES = {
    'aster_l1t_vnir': {'include_only': {'ASTER', 'CORRECT', 'EAST'}},
    'aster_l1t_tir': {'exclude': {'BAND', 'CALENDAR'}}
}


def uncompress_xz(in_file, dest_file):
    with lzma.open(in_file, 'rb') as fin, open(dest_file, 'wb') as fout:
        fout.write(fin.read())


@pytest.fixture
def aster_file(tmp_path):
    dest_file = tmp_path / 'AST_L1T_00312102017022934_20171211115854_25347.hdf'
    uncompress_xz(SCRIPTS_TEST_DATA / 'aster' / '2017.12.10' / 'shrunk.hdf.xz',
                  dest_file)

    yield dest_file


def test_product_defs(aster_file):
    """
    Test product definition
    """
    for product in PRODUCTS:
        measurements = raster_to_measurements(aster_file, product)
        for measure in measurements:
            measure.pop('path')  # This is not needed here
        product_def = generate_lpdaac_defn(measurements, product)

        assert product_def['metadata']['product_type'] == product
        # Check all expected band names ['1', '2', '3']
        assert all([a == b for a, b in zip(['1', '2', '3'],
                                           [m['name'] for m in product_def['measurements']])])


def test_vrt_generation(aster_file):
    """
    Test generated VRT string
    """
    import xml.etree.ElementTree as ET
    import xmlschema

    for product in PRODUCTS:
        bands = selected_bands(aster_file, product)
        vrt = generate_vrt(aster_file, bands)

        # Is it valid VRT schema
        xsd = xmlschema.XMLSchema(str(SCRIPTS_TEST_DATA / 'aster/vrt_schema.xsd'))
        xsd.validate(vrt)

        tree = ET.fromstring(vrt)

        assert len(tree.findall('VRTRasterBand')) == len(PRODUCTS[product])
        sources = tree.findall('SourceFilename')
        for source in sources:
            parts = source.text.split(':')
            # We want the source path name to be absolute
            assert aster_file == Path(parts[2])
            assert parts[4] in PRODUCTS[product]


def test_dataset_doc(aster_file):
    """
    Test dataset doc corresponding to the given file.
    """
    for product in PRODUCTS:
        doc = generate_lpdaac_doc(aster_file, product)
        assert doc['grid_spatial']['projection']['spatial_reference']
        assert len(doc['image']['bands']) == len(PRODUCTS[product])


def test_dataset_indexing(dea_index, aster_file):
    """
    Test datacube indexing for each product for the given file
    """

    for product in PRODUCTS:
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
        dataset, err = resolver(doc, vrt_path.as_uri())
        print('the dataset to be indexed: ', dataset)
        dea_index.datasets.add(dataset)
