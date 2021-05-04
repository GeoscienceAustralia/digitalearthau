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
    ])

    assert md_names == expected_mds

    products_names = set(product.name for product in dea_index.products.get_all())
    # Sanity check that it doesn't return duplicates
    assert len(list(dea_index.products.get_all())) == len(products_names)

    # All expected products.

    # There's a few subtle mistakes we've seen before, such as forgetting the yaml document separator
    # between two products, causing the latter product fields to completely override the former.
    assert products_names == {
        'dsm1sv10',
        'high_tide_comp_20p',
        'high_tide_comp_count',
        'low_tide_comp_20p',
        'low_tide_comp_count',
        'item_v2',
        'item_v2_conf',
        'ls5_level1_scene',
        'ls5_nbar_albers',
        'ls5_nbar_scene',
        'ls5_nbart_albers',
        'ls5_nbart_scene',
        'ls5_pq_albers',
        'ls5_pq_scene',
        'ls5_pq_legacy_scene',
        'ls5_satellite_telemetry_data',
        'ls7_level1_scene',
        'ls7_nbar_albers',
        'ls7_nbar_scene',
        'ls7_nbart_albers',
        'ls7_nbart_scene',
        'ls7_pq_albers',
        'ls7_pq_scene',
        'ls7_pq_legacy_scene',
        'ls7_satellite_telemetry_data',
        'ls8_barest_earth_mosaic',
        'ls8_level1_oli_scene',
        'ls8_level1_scene',
        'ls8_nbar_albers',
        'ls8_nbar_oli_albers',
        'ls8_nbar_oli_scene',
        'ls8_nbar_scene',
        'ls8_nbart_albers',
        'ls8_nbart_oli_albers',
        'ls8_nbart_oli_scene',
        'ls8_nbart_scene',
        'ls8_pq_albers',
        'ls8_pq_oli_albers',
        'ls8_pq_oli_scene',
        'ls8_pq_scene',
        'ls8_pq_legacy_scene',
        'ls8_pq_legacy_oli_scene',
        'ls8_satellite_telemetry_data',
        'pq_count_summary',
        'pq_count_annual_summary',
        's2b_level1c_granule',
        's2a_sen2cor_granule',
        's2b_ard_granule',
        's2a_level1c_granule',
        's2a_ard_granule',
        'ls5_nbart_geomedian_annual',
        'ls7_nbart_geomedian_annual',
        'ls8_nbart_geomedian_annual',
        'wofs_albers',
        'ls5_fc_albers',
        'ls7_fc_albers',
        'ls8_fc_albers',
        'fc_percentile_albers_annual',
        'fc_percentile_albers_seasonal',
        'mangrove_extent_cover_albers',
        'wofs_summary',
        'wofs_filtered_summary',
        'wofs_annual_summary',
        'wofs_nov_mar_summary',
        'wofs_apr_oct_summary',
        'ls5_nbart_tmad_annual',
        'ls7_nbart_tmad_annual',
        'ls8_nbart_tmad_annual',
        's2_tsmask',

        # EO3
        'usgs_ls5t_level1_1',
        'usgs_ls7e_level1_1',
        'usgs_ls7e_level1_2',
        'usgs_ls8c_level1_1',
        "usgs_ls8c_level1_2",
        'ga_ls5t_ard_3',
        'ga_ls7e_ard_3',
        'ga_ls8c_ard_3',
        'ga_ls5t_level1_3',
        'ga_ls7e_level1_3',
        'ga_ls8c_level1_3',
    }


# Damien 2021-05-03: C2 ingested products started failing validation, stating that they have duplicate measurements.
# This is from validation improvements in eodatasets3 0.19, being overzealous. It's failing on measurements specifying
# their own name as an alias. Not ideal, but shouldn't be a failure case.
#
# The ingested products are loaded into dea_index, and there's a morph_dataset_type() function that merges the ingest
# configuration with the source product definition, the source products (eg ls5_nbar_scene) have the new
# field name specified as an alias.
@pytest.mark.xfail(strict=True)
def test_products_are_valid(dea_index: Index):
    for product in dea_index.products.get_all():
        validation_messages = [str(m) for m in eodatasets3.validate.validate_product(product.definition)]
        assert validation_messages == [], f"{product.name} has validation issues: {validation_messages}\n {product.definition}"


def test_metadata_type(dea_index):
    # this came from a bug in the ingestion script
    # where the metadata_type specified in the ingest config
    # was not respected in the output product that inherited
    # the metadata_type from the source product instead
    ls8_nbar_albers = dea_index.products.get_by_name('ls8_nbar_albers')
    expected = ls8_nbar_albers.metadata_type.name
    recorded = ls8_nbar_albers.definition['metadata_type']
    assert expected == recorded
