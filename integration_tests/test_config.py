from datacube.index._api import Index


def test_dea_config(dea_index: Index):
    """
    Check that all the product definitions are valid enough to be added, and that they are added.

    """
    md_names = set(md.name for md in dea_index.metadata_types.get_all())
    # Sanity check that it doesn't return duplicates
    assert len(list(dea_index.metadata_types.get_all())) == len(md_names)

    assert md_names == {
        'eo',
        'landsat_l1_scene',
        'landsat_scene',
        'telemetry',
    }

    products_names = set(product.name for product in dea_index.products.get_all())
    # Sanity check that it doesn't return duplicates
    assert len(list(dea_index.products.get_all())) == len(products_names)

    # All expected products.

    # There's a few subtle mistakes we've seen before, such as forgetting the yaml document separator
    # between two products, causing the latter product fields to completely override the former.
    assert products_names == {
        'dsm1sv10',
        'ls5_level1_scene',
        'ls5_nbar_scene',
        'ls5_nbart_scene',
        'ls5_pq_scene',
        'ls5_satellite_telemetry_data',
        'ls7_level1_scene',
        'ls7_nbar_scene',
        'ls7_nbart_scene',
        'ls7_pq_scene',
        'ls7_satellite_telemetry_data',
        'ls8_level1_oli_scene',
        'ls8_level1_scene',
        'ls8_nbar_oli_scene',
        'ls8_nbar_scene',
        'ls8_nbart_oli_scene',
        'ls8_nbart_scene',
        'ls8_pq_oli_scene',
        'ls8_pq_scene',
        'ls8_satellite_telemetry_data',
        'pq_count_albers',
    }
