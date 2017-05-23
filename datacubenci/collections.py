# coding=utf-8

"""
A collection is this case is datacube-query-arguments and a folder-on-disk
that should contain the same set of datasets.

(Our sync script will compare/"sync" the two)
"""
import fnmatch
from pathlib import Path
from typing import Iterable, Optional, Mapping

from datacubenci.index import DatasetPathIndex, MemoryDatasetPathIndex
from datacubenci.utils import simple_object_repr


class Collection:
    def __init__(self,
                 name: str,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 unique: Iterable[str],
                 index: DatasetPathIndex = None,
                 delete_archived_after_days=None,
                 expected_parents=None):
        self.name = name
        self.query = query
        self.base_path = base_path
        self.offset_pattern = offset_pattern
        self.unique = unique
        self.delete_archived_after_days = delete_archived_after_days
        self.expected_parents = expected_parents

        self._index = index

    def __repr__(self):
        return simple_object_repr(self)

    def iter_fs_paths(self):
        return (path.absolute() for path in self.base_path.glob(self.offset_pattern))

    def iter_fs_uris(self):
        for path in self.iter_fs_paths():
            yield path.as_uri()

    def iter_index_uris(self):
        return map(str, self._index.iter_all_uris(self.query))

    @property
    def file_pattern(self):
        return self.base_path.joinpath(self.offset_pattern)


class SceneCollection(Collection):
    def __init__(self,
                 name: str,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 index: Optional[DatasetPathIndex],
                 delete_archived_after_days=None,
                 expected_parents: Iterable[str] = None):
        super().__init__(name, query, base_path, offset_pattern, index=index,
                         unique=('time.lower.day', 'sat_path.lower', 'sat_row.lower'),
                         delete_archived_after_days=delete_archived_after_days,
                         expected_parents=expected_parents)


# type: Mapping[str, Collection]
_COLLECTIONS = {}


def _add(*cs: Collection):
    for c in cs:
        _COLLECTIONS[c.name] = c


def get_collection(name: str) -> Optional[Collection]:
    return _COLLECTIONS.get(name)


def get_collections() -> Iterable[Collection]:
    return _COLLECTIONS.values()


def registered_collection_names():
    return list(_COLLECTIONS.keys())


def get_collections_in_path(p: Path) -> Iterable[Collection]:
    """
    Get any collections that may have datasets within the given path.

    >>> init_nci_collections(MemoryDatasetPathIndex())
    >>> [c.name for c in get_collections_in_path(Path('/g/data/v10/repackaged'))]
    ['telemetry']
    >>> [c.name for c in get_collections_in_path(Path('/g/data/v10/reprocess/ls8/level1/2016/04'))]
    ['ls8_level1_scene']
    >>> [c.name for c in get_collections_in_path(Path('/g/data/some/fake/path'))]
    []
    """
    for c in get_collections():
        pat = Path(c.file_pattern)
        if fnmatch.fnmatch(str(p), str(pat)) or \
                any(fnmatch.fnmatch(str(p), str(subpat)) for subpat in pat.parents):
            yield c


def init_nci_collections(index: DatasetPathIndex):
    # NCI collections. TODO: move these to config file?

    _add(
        Collection(
            name='telemetry',
            query={'metadata_type': 'telemetry'},
            base_path=Path('/g/data/v10/repackaged/rawdata/0'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/*/ga-metadata.yaml",
            unique=('time.lower.day', 'platform'),
            index=index
        )
    )

    # Level 1
    # /g/data/v10/reprocess/ls7/level1/2016/06/
    #           LS7_ETM_SYS_P31_GALPGS01-002_103_074_20160617/ga-metadata.yaml
    _add(
        SceneCollection(
            name='ls8_level1_scene',
            query={'product': ['ls8_level1_scene', 'ls8_level1_oli_scene']},
            base_path=Path('/g/data/v10/reprocess/ls8/level1'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls7_level1_scene',
            query={'product': 'ls7_level1_scene'},
            base_path=Path('/g/data/v10/reprocess/ls7/level1'),
            offset_pattern="[0.-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls5_level1_scene',
            query={'product': 'ls5_level1_scene'},
            base_path=Path('/g/data/v10/reprocess/ls5/level1'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
            index=index,
        )
    )

    # NBAR & NBART Scenes:
    # /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/08/output/nbar/
    #           LS7_ETM_NBAR_P54_GANBAR01-002_089_078_20040816/ga-metadata.yaml
    # /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/07/output/nbart/
    #           LS7_ETM_NBART_P54_GANBART01-002_114_078_20040731/ga-metadata.yaml
    _add(
        SceneCollection(
            name='ls5_nbar_scene',
            query={'product': ['ls5_nbar_scene', 'ls5_nbart_scene']},
            base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls5'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls7_nbar_scene',
            query={'product': ['ls7_nbar_scene', 'ls7_nbart_scene']},
            base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls7'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls8_nbar_scene',
            query={'product': ['ls8_nbar_scene', 'ls8_nbart_scene']},
            base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls8'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
            index=index,
        )
    )

    # PQ Scenes
    # /g/data/rs0/scenes/pq-scenes-tmp/ls7/2005/01/output/pqa/
    #           LS7_ETM_PQ_P55_GAPQ01-002_108_075_20050113/ga-metadata.yaml
    _add(
        SceneCollection(
            name='ls5_pq_scene',
            query={'product': 'ls5_pq_scene'},
            base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls5'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls7_pq_scene',
            query={'product': 'ls7_pq_scene'},
            base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls7'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
            index=index,
        ),
        SceneCollection(
            name='ls8_pq_scene',
            query={'product': 'ls8_pq_scene'},
            base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls8'),
            offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
            index=index,
        )
    )

    # Example: ingested fractional cover:
    # LS5_TM_FC  LS7_ETM_FC  LS8_OLI_FC
    # /g/data/fk4/datacube/002/LS5_TM_FC/13_-22/LS5_TM_FC_3577_13_-22_20030901235428500000_v1490733226.nc

    def add_albers_collections(name: str, project='rs0'):
        _add(
            Collection(
                name='ls5_{}'.format(name),
                query={'product': 'ls5_{}_albers'.format(name)},
                base_path=Path('/g/data/{}/datacube/002/LS5_TM_{}'.format(project, name.upper())),
                offset_pattern="*_*/LS5*{}*.nc".format(name.upper()),
                unique=('time.lower.day', 'lat', 'lon'),
                index=index,
            ),
            Collection(
                name='ls7_{}'.format(name),
                query={'product': 'ls7_{}_albers'.format(name)},
                base_path=Path('/g/data/{}/datacube/002/LS7_ETM_{}'.format(project, name.upper())),
                offset_pattern="*_*/LS7*{}*.nc".format(name.upper()),
                unique=('time.lower.day', 'lat', 'lon'),
                index=index,
            ),
            Collection(
                name='ls8_{}'.format(name),
                query={'product': 'ls8_{}_albers'.format(name)},
                base_path=Path('/g/data/{}/datacube/002/LS8_OLI_{}'.format(project, name.upper())),
                offset_pattern="*_*/LS8*{}*.nc".format(name.upper()),
                unique=('time.lower.day', 'lat', 'lon'),
                index=index,
            )
        )

    add_albers_collections('pq')
    add_albers_collections('nbar')
    add_albers_collections('nbart')
    add_albers_collections('fc', project='fk4')

    assert get_collection('ls5_fc').base_path == Path('/g/data/fk4/datacube/002/LS5_TM_FC')
    assert get_collection('ls5_fc').offset_pattern == "*_*/LS5*FC*.nc"
    assert get_collection('ls8_nbar').base_path == Path('/g/data/rs0/datacube/002/LS8_OLI_NBAR')
