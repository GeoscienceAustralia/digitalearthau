import uuid
from datetime import datetime
from functools import lru_cache
from typing import Iterable

from datacube.index._api import Index
from datacube.model import Dataset
from datacube.scripts import dataset as dataset_script
from datacube.utils import uri_to_local_path
from digitalearthau.utils import simple_object_repr


class DatasetLite:
    """
    A small subset of datacube.model.Dataset.

    A "real" dataset needs a lot of initialisation: types etc, so this is easier to test with.

    We also, in this script, depend heavily on the __eq__ behaviour of this particular class (by id only), and subtle
    bugs could occur if the core framework made changes to it.
    """

    def __init__(self, id_: uuid.UUID, archived_time: datetime = None) -> None:
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


def add_dataset(index: Index, dataset_id: uuid.UUID, uri: str):
    """
    Index a dataset from a file uri.

    A better api should be pushed upstream to core: it currently only has a "scripts" implementation
    intended for cli use.
    """
    path = uri_to_local_path(uri)
    for d in dataset_script.load_datasets([path], _get_rules(index)):
        if d.id == dataset_id:
            index.datasets.add(d, sources_policy='ensure')
            break
    else:
        raise RuntimeError('Dataset not found at path: %s, %s' % (dataset_id, uri))


def get_datasets_for_uri(index: Index, uri: str) -> Iterable[DatasetLite]:
    """Get all datasets at the given uri"""
    for d in index.datasets.get_datasets_for_location(uri=uri):
        yield DatasetLite.from_agdc(d)


@lru_cache()
def _get_rules(index):
    return dataset_script.load_rules_from_types(index)
