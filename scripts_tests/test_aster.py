import tempfile
import shutil
from pathlib import Path
import pytest
from datacube.index.hl import Doc2Dataset

from scripts.index_aster_lpdaac import generate_lpdaac_defn, generate_lpdaac_doc, generate_vrt, selected_bands
from scripts.index_aster_lpdaac import raster_to_measurements, vrt_file_path

from digitalearthau.testing import factories
from digitalearthau.testing.plugin import local_config, integration_config_paths, INTEGRATION_DEFAULT_CONFIG_PATH
module_db = factories.db_fixture("local_config", scope="module")
module_index = factories.index_fixture("module_db", scope="module")
module_dea_index = factories.dea_index_fixture("module_index", scope="module")

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


def test_dataset_indexing(module_dea_index, aster_file):
    """
    Test datacube indexing for each product for the given file
    """

    with aster_file as file_path:
        for product in PRODUCTS:
            vrt_path = vrt_file_path(file_path, product)
            measurements = raster_to_measurements(file_path, product)
            for measure in measurements:
                measure.pop('path')  # This is not needed here
            product_def = generate_lpdaac_defn(measurements, product)
            product_ = module_dea_index.products.from_doc(product_def)
            indexed_product = module_dea_index.products.add(product_)

            assert indexed_product

            doc = generate_lpdaac_doc(file_path, product)
            resolver = Doc2Dataset(module_dea_index)
            dataset, err = resolver(doc, vrt_path.as_uri())
            print('the dataset to be indexed: ', dataset)
            module_dea_index.datasets.add(dataset)


