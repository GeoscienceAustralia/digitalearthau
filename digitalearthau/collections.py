# coding=utf-8

"""
A collection is this case is datacube-query-arguments and a folder-on-disk
that should contain the same set of datasets.

(Our sync script will compare/"sync" the two)
"""
import fnmatch
import glob
from enum import Enum, auto
from pathlib import Path
from typing import Iterable, Optional, List, Dict, NamedTuple, Sequence

from datacube.index import Index


class Trust(Enum):
    """
    Which side do we trust in a sync? The disk, or the index?
    'None' means don't do anything when there's an unknown dataset (they'll have to be indexed elsewhere)
    """
    NOTHING = auto()
    INDEX = auto()
    DISK = auto()


class Collection(NamedTuple):
    name: str
    # The query args needed to get all of this collection from the datacube index
    query: dict
    # The file glob patterns to iterate all files on disk (NCI collections are all file:// locations)
    file_patterns: Sequence[str]

    index_: Index = None

    # The fields that together uniquely identify a dataset (for finding duplicates)
    unique: Sequence[str] = None

    # If something is archived, how many days before we can delete it? None means never
    delete_archived_after_days: float = None

    trust: Trust = Trust.NOTHING

    def iter_fs_paths(self):
        return (
            Path(path).absolute()
            for file_pattern in self.file_patterns
            for path in glob.iglob(file_pattern)
        )

    def iter_fs_uris(self):
        for path in self.iter_fs_paths():
            yield path.as_uri()

    def iter_index_uris(self):
        """
        Iter over all uris in the index of this collection.

        Both active and archived uris are returned.
        """
        for uri, in self.index_.datasets.search_returning(['uri'], **self.query):
            yield str(uri)

    def constrained_file_patterns(self, within_path: Path) -> List[str]:
        """
        Constrain the file glob pattern(s) to only match datasets within the given folder.

        >>> init_nci_collections(None)
        >>> get_collection('telemetry').constrained_file_patterns(Path('/g/data/v10/repackaged'))
        ['/g/data/v10/repackaged/rawdata/0/[0-9][0-9][0-9][0-9]/[0-9][0-9]/*/ga-metadata.yaml']
        >>> get_collection('ls8_level1_scene').constrained_file_patterns(
        ...     Path('/g/data/v10/reprocess/ls8/level1/2016/04')
        ... )
        ['/g/data/v10/reprocess/ls8/level1/2016/04/LS*/ga-metadata.yaml']
        >>> # Constrain all the way: a specific dataset.
        >>> get_collection('telemetry').constrained_file_patterns(
        ...     Path('/g/data/v10/repackaged/rawdata/0/2016/04/LS8_SOMETHING/ga-metadata.yaml')
        ... )
        ['/g/data/v10/repackaged/rawdata/0/2016/04/LS8_SOMETHING/ga-metadata.yaml']
        >>> get_collection('ls8_level1_scene').constrained_file_patterns(Path('/g/data/some/fake/path'))
        Traceback (most recent call last):
        ...
        ValueError: Folder does not match collection 'ls8_level1_scene': /g/data/some/fake/path
        >>> # Collection has two patterns, only one matches.
        >>> get_collection('ls8_nbar_scene').constrained_file_patterns(
        ...     Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2015/01/output/nbar')
        ... )
        ['/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2015/01/output/nbar/LS*/ga-metadata.yaml']
        """
        out = []
        for pat in self.file_patterns:
            pattern = _constrain_pattern(within_path, pat)
            if pattern:
                out.append(pattern)

        if not out:
            raise ValueError('Folder does not match collection {!r}: {}'.format(self.name, within_path))

        return out

    def iter_fs_paths_within(self, p: Path):
        """
        Iterate over all filesystem paths of this collection that are inside the given folder
        """
        return (
            Path(path).absolute()
            for file_pattern in self.constrained_file_patterns(p)
            for path in glob.iglob(file_pattern)
        )

    # Treated as singletons
    def __eq__(self, o: object) -> bool:
        return self is o

    def __hash__(self):
        return hash(self.name)


def _constrain_pattern(within_path: Path, pattern: str):
    """
    >>> _constrain_pattern(Path('/tmp/test'), '/tmp/test/[0-9]')
    '/tmp/test/[0-9]'
    >>> _constrain_pattern(Path('/tmp/test-5'), '/tmp/test-[0-9]/[0-9]/file.txt')
    '/tmp/test-5/[0-9]/file.txt'
    >>> # Constrain all the way.
    >>> _constrain_pattern(Path('/tmp/test/09'), '/tmp/test/[0-9][0-9]')
    '/tmp/test/09'
    >>> # Doesn't match pattern: None
    >>> _constrain_pattern(Path('/tmp/non-matching-dir'), '/tmp/test/[0-9][0-9]')
    """
    # Does it match the whole pattern? Return verbatim (constrained all the way)
    if fnmatch.fnmatch(str(within_path), pattern):
        return str(within_path)
    else:
        pathtern = Path(pattern)
        # Otherwise move up the directory tree until we find a matching base
        suffix = [pathtern.name]
        for subpat in pathtern.parents:
            if fnmatch.fnmatch(str(within_path), str(subpat)):
                return str(within_path.joinpath(*reversed(suffix)))
            else:
                suffix.append(subpat.name)
    return None


_COLLECTIONS = {}  # type: Dict[str, Collection]


def _add(*cs: Collection):
    for c in cs:
        _COLLECTIONS[c.name] = c


def get_collection(name: str) -> Optional[Collection]:
    return _COLLECTIONS.get(name)


def get_collections() -> Iterable[Collection]:
    return list(_COLLECTIONS.values())


def registered_collection_names():
    return list(_COLLECTIONS.keys())


def get_collections_in_path(p: Path) -> Iterable[Collection]:
    """
    Get any collections that may have datasets within the given path.

    >>> init_nci_collections(None)
    >>> [c.name for c in get_collections_in_path(Path('/g/data/v10/repackaged'))]
    ['telemetry']
    >>> [c.name for c in get_collections_in_path(Path('/g/data/v10/reprocess/ls8/level1/2016/04'))]
    ['ls8_level1_scene']
    >>> [c.name for c in get_collections_in_path(Path('/g/data/some/fake/path'))]
    []
    >>> [c.name for c in get_collections_in_path(Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls8/2015/01/output/nbar'))]
    ['ls8_nbar_scene']
    """
    for c in get_collections():
        for pat in c.file_patterns:
            # Match either the whole pattern parent folders of it.
            if fnmatch.fnmatch(str(p), str(pat)) or \
                    any(fnmatch.fnmatch(str(p), str(subpat)) for subpat in Path(pat).parents):
                yield c
                # Break from the pattern loop so that we don't return the same collection multiple times
                break


def init_nci_collections(index: Index):
    # NCI collections. TODO: move these to config file?

    _add(
        Collection(
            name='telemetry',
            query={'metadata_type': 'telemetry'},
            file_patterns=(
                '/g/data/v10/repackaged/rawdata/0/[0-9][0-9][0-9][0-9]/[0-9][0-9]/*/ga-metadata.yaml',
            ),
            unique=('time.lower.day', 'platform'),
            index_=index,
            trust=Trust.DISK
        )
    )

    def scene_collection(name, query, file_patterns, delete_archived_after_days=None):
        """Make a collection with common defaults for scene collections"""
        return Collection(
            name,
            query,
            file_patterns=file_patterns,
            index_=index,
            unique=('time.lower.day', 'sat_path.lower', 'sat_row.lower'),
            delete_archived_after_days=delete_archived_after_days,
            # Scenes default to trusting disk. They're atomically written to the destination,
            # and the jobs themselves wont index.
            trust=Trust.DISK
        )

    # Level 1
    # /g/data/v10/reprocess/ls7/level1/2016/06/
    #           LS7_ETM_SYS_P31_GALPGS01-002_103_074_20160617/ga-metadata.yaml
    _add(
        scene_collection(
            name='ls8_level1_scene',
            query={'product': ['ls8_level1_scene', 'ls8_level1_oli_scene']},
            file_patterns=[
                '/g/data/v10/reprocess/ls8/level1/[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml',
            ],
        ),
        scene_collection(
            name='ls7_level1_scene',
            query={'product': 'ls7_level1_scene'},
            file_patterns=[
                '/g/data/v10/reprocess/ls7/level1/[0.-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml',
            ],
        ),
        scene_collection(
            name='ls5_level1_scene',
            query={'product': 'ls5_level1_scene'},
            file_patterns=[
                '/g/data/v10/reprocess/ls5/level1/[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml',
            ],
        )
    )

    # NBAR & NBART Scenes:
    # /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/08/output/nbar/
    #           LS7_ETM_NBAR_P54_GANBAR01-002_089_078_20040816/ga-metadata.yaml
    # /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/07/output/nbart/
    #           LS7_ETM_NBART_P54_GANBART01-002_114_078_20040731/ga-metadata.yaml
    nbar_scene_offset = '[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml'
    _add(
        scene_collection(
            name='ls5_nbar_scene',
            query={'product': ['ls5_nbar_scene', 'ls5_nbart_scene']},
            file_patterns=[
                '/g/data/rs0/scenes/nbar-scenes-tmp/ls5/' + nbar_scene_offset,
                '/short/v10/scenes/nbar-scenes-tmp/ls5/' + nbar_scene_offset,
            ],
        ),
        scene_collection(
            name='ls7_nbar_scene',
            query={'product': ['ls7_nbar_scene', 'ls7_nbart_scene']},
            file_patterns=[
                '/g/data/rs0/scenes/nbar-scenes-tmp/ls7/' + nbar_scene_offset,
                '/short/v10/scenes/nbar-scenes-tmp/ls7/' + nbar_scene_offset,
            ],
        ),
        scene_collection(
            name='ls8_nbar_scene',
            query={'product': ['ls8_nbar_scene', 'ls8_nbart_scene']},
            file_patterns=[
                '/g/data/rs0/scenes/nbar-scenes-tmp/ls8/' + nbar_scene_offset,
                '/short/v10/scenes/nbar-scenes-tmp/ls8/' + nbar_scene_offset,
            ],
        )
    )

    # PQ Scenes
    # /g/data/rs0/scenes/pq-scenes-tmp/ls7/2005/01/output/pqa/
    #           LS7_ETM_PQ_P55_GAPQ01-002_108_075_20050113/ga-metadata.yaml
    _add(
        scene_collection(
            name='ls5_pq_scene',
            query={'product': 'ls5_pq_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-scenes-tmp/ls5/[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml',
            ],
        ),
        scene_collection(
            name='ls7_pq_scene',
            query={'product': 'ls7_pq_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-scenes-tmp/ls7/[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml',
            ],
        ),
        scene_collection(
            name='ls8_pq_scene',
            query={'product': 'ls8_pq_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-scenes-tmp/ls8/[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml',
            ],
        )
    )

    # Legacy PQ Scenes
    # /g/data/rs0/scenes/pq-legacy-scenes-tmp/ls7/2005/01/output/pqa/
    #           LS7_ETM_PQ_P55_GAPQ01-002_108_075_20050113/ga-metadata.yaml

    pq_scene_offset = '[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml'
    _add(
        scene_collection(
            name='ls5_pq_legacy_scene',
            query={'product': 'ls5_pq_legacy_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls5/' + pq_scene_offset,
            ],
        ),
        scene_collection(
            name='ls7_pq_legacy_scene',
            query={'product': 'ls7_pq_legacy_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls7/' + pq_scene_offset,
            ],
        ),
        scene_collection(
            name='ls8_pq_legacy_scene',
            query={'product': 'ls8_pq_legacy_scene'},
            file_patterns=[
                '/g/data/rs0/scenes/pq-legacy-scenes-tmp/ls8/' + pq_scene_offset,
            ],
        )
    )

    # Example: ingested fractional cover:
    # LS5_TM_FC  LS7_ETM_FC  LS8_OLI_FC
    # /g/data/fk4/datacube/002/LS5_TM_FC/13_-22/LS5_TM_FC_3577_13_-22_20030901235428500000_v1490733226.nc

    def add_albers_collections(name: str, project='rs0'):
        _add(
            Collection(
                name='ls5_{}_albers'.format(name),
                query={'product': 'ls5_{}_albers'.format(name)},
                file_patterns=(
                    '/g/data/{project}/datacube/002/'
                    'LS5_TM_{name}/*_*/LS5*{name}*.nc'.format(project=project,
                                                              name=name.upper()),
                ),
                unique=('time.lower.day', 'lat', 'lon'),
                index_=index,
                # Tiles default to trusting index over the disk: they were indexed at the end of the job,
                # so unfinished tiles could be left on disk.
                trust=Trust.INDEX
            ),
            Collection(
                name='ls7_{}_albers'.format(name),
                query={'product': 'ls7_{}_albers'.format(name)},
                file_patterns=(
                    '/g/data/{project}/datacube/002/LS7_ETM_{name}/'
                    '*_*/LS7*{name}*.nc'.format(project=project,
                                                name=name.upper()),
                ),
                unique=('time.lower.day', 'lat', 'lon'),
                index_=index,
                # Tiles default to trusting index over the disk: they were indexed at the end of the job,
                # so unfinished tiles could be left on disk.
                trust=Trust.INDEX
            ),
            Collection(
                name='ls8_{}_albers'.format(name),
                query={'product': 'ls8_{}_albers'.format(name)},
                file_patterns=(
                    '/g/data/{project}/datacube/002/LS8_OLI_{name}/'
                    '*_*/LS8*{name}*.nc'.format(project=project,
                                                name=name.upper()),
                ),
                unique=('time.lower.day', 'lat', 'lon'),
                index_=index,
                # Tiles default to trusting index over the disk: they were indexed at the end of the job,
                # so unfinished tiles could be left on disk.
                trust=Trust.INDEX
            )
        )

    add_albers_collections('pq')
    add_albers_collections('nbar')
    add_albers_collections('nbart')

    # Old FC
    for sat in ['ls5', 'ls7', 'ls8']:
        name = 'fc'
        glob_offset = f'{sat.upper()}_TM_{name.upper()}/*_*/{sat.upper()}*{name.upper()}*.nc'
        _add(
            Collection(
                name=f'{sat}_{name}_albers',
                query={'product': '{sat}_{name}_albers'},
                file_patterns=(
                    f'/g/data/fk4/datacube/002/' + glob_offset,
                    f'/g/data/fk4/datacube/.trash/' + glob_offset,
                ),
                unique=('time.lower.day', 'lat', 'lon'),
                # Tiles default to trusting index over the disk: they were indexed at the end of the job,
                # so unfinished tiles could be left on disk.
                trust=Trust.INDEX
            )
        )
    # New FC
    for sat in ['ls5', 'ls7', 'ls8']:
        name = 'fc'
        glob_offset = f'{sat.upper()}_TM_{name.upper()}/*_*/{sat.upper()}*{name.upper()}*.nc'
        _add(
            Collection(
                name=f'{sat}_{name}_albers_staging',
                query={'product': '{sat}_{name}_albers_staging'},
                file_patterns=(
                    f'/g/data/v10/public/data/fc/' + glob_offset,
                    f'/g/data/fk4/datacube/002/' + glob_offset,
                ),
                unique=('time.lower.day', 'lat', 'lon'),
                # Tiles default to trusting index over the disk: they were indexed at the end of the job,
                # so unfinished tiles could be left on disk.
                trust=Trust.INDEX
            )
        )

    # PQ stats
    _add(
        Collection(
            name='pq_count_summary',
            query={'product': 'pq_count_summary'},
            file_patterns=['/g/data/fk4/datacube/002/stats/pq_count/history/LS_PQ_COUNT/*_*/LS_PQ_COUNT_3577_*.nc'],
            index_=index,
            unique=('time.lower.day', 'lat', 'lon')
        )
    )
    _add(
        Collection(
            name='pq_count_annual_summary',
            query={'product': 'pq_count_annual_summary'},
            index_=index,
            file_patterns=['/g/data/fk4/datacube/002/stats/pq_count/annual/LS_PQ_COUNT/*_*/LS_PQ_COUNT_3577_*.nc'],
            unique=('time.lower.day', 'lat', 'lon')
        )
    )

    assert '/g/data/fk4/datacube/002/LS5_TM_FC/*_*/LS5*FC*.nc' in get_collection('ls5_fc_albers').file_patterns
    assert get_collection('ls8_nbar_albers').file_patterns == (
        '/g/data/rs0/datacube/002/LS8_OLI_NBAR/*_*/LS8*NBAR*.nc',
    )
