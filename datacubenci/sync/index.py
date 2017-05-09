import uuid
from datetime import datetime
from typing import Iterable, Any, Mapping, Optional

from datacube.index import index_connect
from datacube.index._api import Index
from datacube.model import Dataset
from datacube.scripts import dataset as dataset_script
from datacube.utils import uri_to_local_path
from datacubenci.collections import simple_object_repr


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

    def iter_all_uris(self) -> Iterable[str]:
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


class AgdcDatasetPathIndex(DatasetPathIndex):
    def __init__(self, index: Index, query: dict):
        super().__init__()
        self._index = index
        self._query = query
        self._rules = dataset_script.load_rules_from_types(self._index)

    def iter_all_uris(self) -> Iterable[str]:
        for uri, in self._index.datasets.search_returning(['uri'], **self._query):
            yield str(uri)

    @classmethod
    def connect(cls, query: Mapping[str, Any]) -> 'AgdcDatasetPathIndex':
        return cls(index_connect(application_name='datacubenci-pathsync'), query=query)

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

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self._index.close()
