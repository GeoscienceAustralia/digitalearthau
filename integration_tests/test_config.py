import eodatasets3.validate
import pytest

from datacube.index import Index


def test_dea_config(dea_index: Index):
    """
    Check that all the product definitions are valid enough to be added, and that they are added.
    """
    md_names = sorted(md.name for md in dea_index.metadata_types.get_all())
    # Sanity check that it doesn't return duplicates
    # assert len(list(dea_index.metadata_types.get_all())) == len(md_names)

    expected_mds = sorted([
        'eo',
        'gqa_eo',
        'landsat_l1_scene',
        'landsat_scene',
        'telemetry',
        'eo3',
        'eo3_landsat_ard',
        'eo3_landsat_l1',
        'eo3_sentinel',
        'eo3_sentinel_ard',
    ])

    assert md_names == expected_mds

    products_names = set(product.name for product in dea_index.products.get_all())
    # Sanity check that it doesn't return duplicates
    assert len(list(dea_index.products.get_all())) == len(products_names)

    # Make sure some were indexed.
    # (We're not exact, as we don't want to bump this constantly as products are added)
    assert len(products_names) >= 80


# Damien 2021-05-03: C2 ingested products started failing validation, stating that they have duplicate
# measurements. This is from validation improvements in eodatasets3 0.19, being overzealous. It's failing
# on measurements specifying their own name as an alias. Not ideal, but shouldn't be a failure case.
#
# The ingested products are loaded into dea_index, and there's a morph_dataset_type()
# function that merges the ingest configuration with the source product definition, the
# source products (eg ls5_nbar_scene) have the new field name specified as an alias.
@pytest.mark.xfail(strict=True)
def test_products_are_valid(dea_index: Index):
    for product in dea_index.products.get_all():
        validation_messages = [str(m) for m in eodatasets3.validate.validate_product(product.definition)]
        assert validation_messages == [], f"{product.name} has validation issues: " \
                                          f"{validation_messages}\n {product.definition}"


def test_metadata_type(dea_index):
    # this came from a bug in the ingestion script
    # where the metadata_type specified in the ingest config
    # was not respected in the output product that inherited
    # the metadata_type from the source product instead
    ls8_nbar_albers = dea_index.products.get_by_name('ls8_nbar_albers')
    expected = ls8_nbar_albers.metadata_type.name
    recorded = ls8_nbar_albers.definition['metadata_type']
    assert expected == recorded
