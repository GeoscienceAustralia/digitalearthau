from datacubenci import sync
from datacubenci.paths import write_files


def test_iter_product_pathsets():
    root = write_files(
        {
            'ls8_scenes': {
                'ls8_test_dataset': {
                    'ga-metadata.yaml':
                        'id: 1e47df58-de0f-11e6-93a4-185e0f80a5c0\n',
                    'otherfile.txt': ''
                }
            },
            'ls7_scenes': {

            },
            'dummy_dataset': {
                'ga-metadata.yaml': ''
            }

        }
    )

    products = {
        'ls8_level1_scene': root.joinpath('ls8_scenes'),
        'ls7_level1_scene': root.joinpath('ls7_scenes'),
    }

    expected_paths = {
        'ls7_level1_scene': [],
        'ls8_level1_scene': [root.joinpath('ls8_scenes', 'ls8_test_dataset', 'ga-metadata.yaml')]
    }

    cache_path = root.joinpath('cache')
    product_return_count = 0
    for product, path_set in sync.iter_product_pathsets(products, cache_path):
        product_return_count += 1
        expected = expected_paths.pop(product, None)
        assert expected is not None, "Product {} not expected (again?)".format(product)

        # All the paths we expect should be there.
        for expected_path in expected:
            assert expected_path.absolute().as_uri() in path_set

        # A our dummy outside of the product folder should not
        dummy_dataset = root.joinpath('dummy_dataset', 'ga-metadata.yaml')
        assert dummy_dataset.absolute().as_uri() not in path_set

    assert product_return_count == len(products)
