import sys
from pathlib import Path
from typing import Optional
from uuid import UUID

from boltons import strutils
from boltons.jsonutils import JSONLIterator

from digitalearthau.index import DatasetLite
from digitalearthau.utils import simple_object_repr


class Mismatch:
    """
    A mismatch between index and filesystem.

    See the implementations for different types of mismataches.
    """

    def __init__(self, dataset: Optional[DatasetLite], uri: str) -> None:
        super().__init__()
        self.dataset = dataset
        self.uri = uri

    def __repr__(self, *args, **kwargs):
        """
        >>> Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test')
        Mismatch(dataset=DatasetLite(archived_time=None, id=UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), \
uri='/tmp/test')
        """
        return simple_object_repr(self)

    def __eq__(self, other):
        """
        >>> m = Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test')
        >>> m == m
        True
        >>> import copy
        >>> m == copy.copy(m)
        True
        >>> n = Mismatch(DatasetLite(UUID('96519c56-e133-11e6-a29f-185e0f80a5c0')), uri='/tmp/test2')
        >>> m == n
        False
        """
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(v for k, v in sorted(self.__dict__.items())))

    def to_dict(self):
        return dict(
            name=strutils.camel2under(self.__class__.__name__),
            dataset_id=str(self.dataset.id) if self.dataset else None,
            uri=self.uri
        )

    @staticmethod
    def from_dict(row: dict):

        mismatch_class = getattr(sys.modules[__name__], strutils.under2camel(row['name']))
        dataset_id = row['dataset_id'].strip()

        dataset = None
        if dataset_id and dataset_id != 'None':
            dataset = DatasetLite(UUID(dataset_id))

        return mismatch_class(dataset, row['uri'].strip())


class LocationMissingOnDisk(Mismatch):
    """
    The dataset is no longer at the given location.

    (Note that there may still be a file at the location, but it is not this dataset)
    """
    pass


class LocationNotIndexed(Mismatch):
    """
    An existing dataset has been found at a new location.
    """
    pass


class DatasetNotIndexed(Mismatch):
    """
    A dataset on the filesystem is not in the index.
    """
    pass


class ArchivedDatasetOnDisk(Mismatch):
    """
    A dataset on disk is archived in the index.
    """
    pass


class UnreadableDataset(Mismatch):
    """
    An error was returned when reading a dataset.

    We can't currently easily separate whether this is a temporary system/disk error or an actual corrupt dataset.
    """
    pass


class InvalidDataset(Mismatch):
    """
    An error was returned from validation
    """
    pass


def mismatches_from_file(path: Path):
    """
    Load mismatches from a json lines file
    """
    with path.open('r') as f:
        for row in JSONLIterator(f):
            if not row:
                continue

            yield Mismatch.from_dict(row)
