import uuid
import structlog

from datetime import datetime
from typing import Iterable
from datacube.index import Index
from datacube.model import Dataset
from datacube.utils import uri_to_local_path
from digitalearthau.utils import simple_object_repr
from datacube.ui.common import ui_path_doc_stream
from datacube.index.hl import Doc2Dataset, check_dataset_consistent

_LOG = structlog.getLogger('dea-dataset')


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
    yaml_path = uri_to_local_path(uri)

    def load_datasets(path, ds_resolve):
        for uri, ds in ui_path_doc_stream(path):

            dataset, err = ds_resolve(ds, uri)

            if dataset is None:
                _LOG.error('%s', str(err))
                continue

            is_consistent, reason = check_dataset_consistent(dataset)
            if not is_consistent:
                _LOG.error("Dataset %s inconsistency: %s", dataset.id, reason)
                continue

            yield dataset

    ds_resolve = Doc2Dataset(index)

    for d in load_datasets([yaml_path], ds_resolve):
        if d.id == dataset_id:
            try:
                index.datasets.add(d)
                _LOG.info("dataset indexing successful", dataset_id=dataset_id)
                break
            except ValueError as err:
                _LOG.error('failed to index dataset', dataset_id=dataset_id, error=err)
    else:
        raise RuntimeError('dataset not found at path: %s, %s' % (dataset_id, uri))


def get_datasets_for_uri(index: Index, uri: str) -> Iterable[DatasetLite]:
    """Get all datasets at the given uri"""
    for d in index.datasets.get_datasets_for_location(uri=uri):
        yield DatasetLite.from_agdc(d)
