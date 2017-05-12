from typing import Optional
from uuid import UUID

from datacubenci.index import DatasetLite
from datacubenci.utils import simple_object_repr


class Mismatch:
    """
    A mismatch between index and filesystem.

    See the implementations for different types of mismataches.
    """

    def __init__(self, dataset: Optional[DatasetLite], uri: str):
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
