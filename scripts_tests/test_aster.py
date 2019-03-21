import tempfile
import shutil
from pathlib import Path
import pytest

from scripts.index_aster_lpdaac import generate_lpdaac_defn, generate_lpdaac_doc, generate_vrt, selected_bands
from scripts.index_aster_lpdaac import raster_to_measurements

SCRIPTS_TEST_DATA = Path(__file__).parent / 'data'

PRODUCTS = {'aster_l1t_vnir': {'ImageData2', 'ImageData1', 'ImageData3N'},
            'aster_l1t_tir': {'ImageData10', 'ImageData11', 'ImageData12', 'ImageData13', 'ImageData14'}}

EXTRA_METADATA_PREFIXES = {
    'aster_l1t_vnir': {'include_only': {'ASTER', 'CORRECT', 'EAST'}},
    'aster_l1t_tir': {'exclude': {'BAND', 'CALENDAR'}}
}


@pytest.fixture
def aster_file():
    tempdir = tempfile.TemporaryDirectory()
    shutil.copy2(SCRIPTS_TEST_DATA / 'aster' / '2017.12.10' / 'AST_L1T_00312102017022934_20171211115854_25347.hdf',
                 Path(tempdir.name))

    yield Path(tempdir.name) / 'AST_L1T_00312102017022934_20171211115854_25347.hdf'

    tempdir.cleanup()


def test_product_defs(aster_file):
    """
    Test product definition
    """
    with aster_file as file_path:
        for product in PRODUCTS:
            measurements = raster_to_measurements(file_path, product)
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

    with aster_file as file_path:
        for product in PRODUCTS:
            bands = selected_bands(file_path, product)
            vrt = generate_vrt(file_path, bands)

            # Is it valid VRT schema
            xsd = xmlschema.XMLSchema(f'{SCRIPTS_TEST_DATA.name}/aster/vrt_schema.xsd')
            xsd.validate(vrt)

            tree = ET.fromstring(vrt)

            assert len(tree.findall('VRTRasterBand')) == len(PRODUCTS[product])
            sources = tree.findall('SourceFilename')
            for source in sources:
                parts = source.text.split(':')
                # We want the source path name to be absolute
                assert file_path == Path(parts[2])
                assert parts[4] in PRODUCTS[product]


def test_dataset_doc(aster_file):
    """
    Test dataset doc corresponding to the given file.
    """
    with aster_file as file_path:
        for product in PRODUCTS:
            doc = generate_lpdaac_doc(file_path, product)
            assert doc['grid_spatial']['projection']['spatial_reference']
            assert len(doc['image']['bands']) == len(PRODUCTS[product])
