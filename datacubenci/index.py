import collections
import uuid
from datetime import datetime
from typing import Iterable, Optional, Mapping, List

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Dataset
from datacube.scripts import dataset as dataset_script
from datacube.utils import uri_to_local_path
from datacubenci.utils import simple_object_repr


class DatasetLite:
    """
    A small subset of datacube.model.Dataset.

    A "real" dataset needs a lot of initialisation: types etc, so this is easier to test with.

    We also, in this script, depend heavily on the __eq__ behaviour of this particular class (by id only), and subtle
    bugs could occur if the core framework made changes to it.
    """

    def __init__(self, id_: uuid.UUID, archived_time: datetime = None):
        # Sanity check of the type, as our equality checks are quietly wrong if the types don't match,
        # and we've previously had problems with libraries accidentally switching string/uuid types...
        assert isinstance(id_, uuid.UUID)
        self.id = id_

        self.archived_time = archived_time

    @property
    def is_archived(self):
        """
        Is this dataset archived?

        (an archived dataset is one that is not intended to be used by users anymore: eg. it has been
        replaced by another dataset. It will not show up in search results, but still exists in the
        system via provenance chains or through id lookup.)

        :rtype: bool
        """
        return self.archived_time is not None

    def __eq__(self, other):
        if not other:
            return False

        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def from_agdc(cls, dataset: Dataset):
        return DatasetLite(dataset.id, archived_time=dataset.archived_time)

    def __repr__(self):
        return simple_object_repr(self)


class DatasetPathIndex:
    """
    An index of datasets and their URIs.

    This is a slightly questionable attempt to make testing/mocking simpler.

    There's two implementations: One in-memory and one that uses a real datacube.
    (MemoryDatasetPathIndex and AgdcDatasetPathIndex)
    """

    def iter_all_uris(self, query: dict) -> Iterable[str]:
        raise NotImplementedError

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        raise NotImplementedError

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        raise NotImplementedError

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        raise NotImplementedError

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:
        raise NotImplementedError

    def add_dataset(self, dataset: DatasetLite, uri: str):
        raise NotImplementedError

    def close(self):
        """Do any clean-up as needed before forking."""
        # Default implementation: no-op
        pass


class AgdcDatasetPathIndex(DatasetPathIndex):
    def __init__(self, index: Index):
        super().__init__()
        self._index = index
        self._rules = dataset_script.load_rules_from_types(self._index)

    def iter_all_uris(self, query: dict) -> Iterable[str]:
        for uri, in self._index.datasets.search_returning(['uri'], **query):
            yield str(uri)

    @classmethod
    def connect(cls) -> 'AgdcDatasetPathIndex':
        return cls(index_connect(application_name='datacubenci-pathsync'))

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        for d in self._index.datasets.get_datasets_for_location(uri=uri):
            yield DatasetLite.from_agdc(d)

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:
        was_removed = self._index.datasets.remove_location(dataset.id, uri)
        return was_removed

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        agdc_dataset = self._index.datasets.get(dataset_id)
        return DatasetLite.from_agdc(agdc_dataset) if agdc_dataset else None

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        was_removed = self._index.datasets.add_location(dataset.id, uri)
        return was_removed

    def add_dataset(self, dataset: DatasetLite, uri: str):
        path = uri_to_local_path(uri)

        for d in dataset_script.load_datasets([path], self._rules):
            if d.id == dataset.id:
                self._index.datasets.add(d, sources_policy='ensure')
                break
        else:
            raise RuntimeError('Dataset not found at path: %s, %s' % (dataset.id, uri))

    def close(self):
        self._index.close()

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()


class MemoryDatasetPathIndex(DatasetPathIndex):
    """
    An in-memory implementation, so that we can test without using a real datacube index.
    """

    def get(self, dataset_id: uuid.UUID) -> Optional[DatasetLite]:
        for d in self._records.keys():
            if d.id == dataset_id:
                return d
        return None

    def __init__(self):
        super().__init__()
        # Map of dataset to locations.
        # type: Mapping[DatasetLite, List[str]]
        self._records = collections.defaultdict(list)

    def reset(self):
        self._records = collections.defaultdict(list)

    def iter_all_uris(self, query: dict) -> Iterable[str]:
        for uris in self._records.values():
            yield from uris

    def add_location(self, dataset: DatasetLite, uri: str) -> bool:
        if dataset not in self._records:
            raise ValueError("Unknown dataset {} -> {}".format(dataset.id, uri))

        return self._add(dataset, uri)

    def _add(self, dataset_id, uri):
        if uri in self._records[dataset_id]:
            # Not added
            return False

        self._records[dataset_id].append(uri)
        return True

    def remove_location(self, dataset: DatasetLite, uri: str) -> bool:

        if uri not in self._records[dataset]:
            # Not removed
            return False
        # We never remove the dataset key, only the uris.
        self._records[dataset].remove(uri)

    def get_datasets_for_uri(self, uri: str) -> Iterable[DatasetLite]:
        for dataset, uris in self._records.items():
            if uri in uris:
                yield dataset

    def as_map(self) -> Mapping[DatasetLite, Iterable[str]]:
        """
        All contained (dataset, [location]) values, to check test results.
        """
        return {id_: tuple(uris) for id_, uris in self._records.items()}

    def add_dataset(self, dataset: DatasetLite, uri: str):
        # We're not actually storing datasets...
        return self._add(dataset, uri)
