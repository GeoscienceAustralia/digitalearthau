import uuid
from typing import Iterable, List

from boltons.dictutils import MultiDict
from datacubenci import sync
from datacubenci.paths import write_files


# In-memory index, so that we can test without using a real datacube index.
class MemoryDatasetPathIndex(sync.DatasetPathIndex):
    def has_dataset(self, dataset_id: uuid.UUID) -> bool:
        # noinspection PyCompatibility
        return dataset_id in self._records.itervalues(multi=True)

    def __init__(self):
        super().__init__()
        # Map paths to lists of dataset ids.
        self._records = MultiDict()

    def iter_all_uris(self, product: str) -> Iterable[str]:
        return self._records.keys()

    def add_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        ids = self._records.getlist(uri)
        if dataset_id in ids:
            # Not added
            return False

        self._records.add(uri, dataset_id)
        return True

    def remove_location(self, dataset_id: uuid.UUID, uri: str) -> bool:
        if dataset_id not in self.get_dataset_ids_for_uri(uri):
            # Not removed
            return False

        ids = self._records.popall(uri)
        ids.remove(dataset_id)
        self._records.addlist(uri, ids)

    def get_dataset_ids_for_uri(self, uri: str) -> List[uuid.UUID]:
        return list(self._records.getlist(uri))


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

    index = MemoryDatasetPathIndex()
    already_indexed_uri = root.joinpath('indexed', 'already', 'ga-metadata.yaml').as_uri()
    index.add_location(uuid.UUID('1e47df58-de0f-11e6-93a4-185e0f80a5c0'),
                       already_indexed_uri)

    products = {
        'ls8_level1_scene': root.joinpath('ls8_scenes'),
        'ls7_level1_scene': root.joinpath('ls7_scenes'),
    }

    expected_paths = {
        'ls7_level1_scene': [
            already_indexed_uri
        ],
        'ls8_level1_scene': [
            root.joinpath('ls8_scenes', 'ls8_test_dataset', 'ga-metadata.yaml').as_uri(),
            already_indexed_uri
        ]
    }

    cache_path = root.joinpath('cache')

    expected_path_count = {
        'ls7_level1_scene': 1,
        'ls8_level1_scene': 2
    }

    # It should find and add all of the on-disk datasets
    product_return_count = 0
    for product, path_set in sync.iter_product_pathsets(products, index, cache_path):
        product_return_count += 1
        expected = expected_paths.pop(product, None)
        assert expected is not None, "Product {} not expected (again?)".format(product)

        # All the paths we expect should be there.
        for expected_path in expected:
            assert expected_path in path_set

        # A our dummy outside of the product folder should not
        dummy_dataset = root.joinpath('dummy_dataset', 'ga-metadata.yaml')
        assert dummy_dataset.absolute().as_uri() not in path_set

        assert sum(1 for p in path_set.iterkeys('file://')) == expected_path_count[product]

    assert product_return_count == len(products)
