import os

import pytest

from digitalearthau.harvest import iso19115

# These are ok in tests.
# pylint: disable=redefined-outer-name


@pytest.fixture
def mapping_yaml():
    return os.path.join(os.path.split(os.path.realpath(__file__))[0], 'mapping.yaml')


@pytest.fixture
def sample_iso19115_doc():
    return os.path.join(os.path.split(os.path.realpath(__file__))[0], 'test_iso19115_doc.xml')


@pytest.fixture
def sample_cmi_doc():
    """Download an ISO19115 document from the live CMI server. (Skip tests if CMI is down)"""
    try:
        url = 'http://cmi.ga.gov.au/ecat/115'
        return iso19115.open_iso_tree(url)
    except OSError:
        pytest.skip("CMI Unavailable")


def test_harvest_load_mapping_table(mapping_yaml):
    mapping_table = iso19115.load_mapping_table(mapping_yaml)

    assert 'title' in list(mapping_table.keys())


def test_harvest_attrs(sample_iso19115_doc, mapping_yaml):
    mapping_table = iso19115.load_mapping_table(mapping_yaml)
    tree = iso19115.open_iso_tree(sample_iso19115_doc)
    global_attrs = iso19115.find_attrs_in_tree(tree, mapping_table)

    assert global_attrs['title'] == 'Surface Reflectance NBAR+T 25 v. 2'
    assert global_attrs['uuid'] == '00cfe910-722c-43e6-a9ed-103eb52bc916'
    assert global_attrs['license'] == 'CC BY Attribution 4.0 International License'


def test_harvest_from_cmi_xml(mapping_yaml, sample_cmi_doc):
    mapping_table = iso19115.load_mapping_table(mapping_yaml)
    global_attrs = iso19115.find_attrs_in_tree(sample_cmi_doc, mapping_table)

    assert 'title' in global_attrs
    assert 'uuid' in global_attrs
    assert 'license' in global_attrs
