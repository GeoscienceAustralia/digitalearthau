# coding=utf-8

"""
A collection is this case is datacube-query-arguments and a folder-on-disk
that should contain the same set of datasets.

(Our sync script will compare/"sync" the two)
"""

from pathlib import Path
from typing import Iterable, Tuple


class Collection:
    def __init__(self,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 unique: Iterable[str],
                 delete_archived_after_days=None,
                 expected_parents=None):
        self.query = query
        self.base_path = base_path
        self.offset_pattern = offset_pattern
        self.unique = unique
        self.delete_archived_after_days = delete_archived_after_days
        self.expected_parents = expected_parents


class SceneCollection(Collection):
    def __init__(self,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 delete_archived_after_days=None,
                 expected_parents: Tuple[str] = None):
        super().__init__(query, base_path, offset_pattern,
                         unique=('sat_path.lower', 'sat_row.lower', 'time.lower.day'),
                         delete_archived_after_days=delete_archived_after_days,
                         expected_parents=expected_parents)


NCI_COLLECTIONS = {
    'telemetry': Collection(
        query={'metadata_type': 'telemetry'},
        base_path=Path('/g/data/v10/repackaged/rawdata/0'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/*/ga-metadata.yaml",
        unique=('platform', 'time.lower.day'),
    ),
}

# Level 1
# /g/data/v10/reprocess/ls7/level1/2016/06/
#           LS7_ETM_SYS_P31_GALPGS01-002_103_074_20160617/ga-metadata.yaml
NCI_COLLECTIONS.update({
    'ls8_level1_scene': SceneCollection(
        query={'product': ['ls8_level1_scene', 'ls8_level1_oli_scene']},
        base_path=Path('/g/data/v10/reprocess/ls8/level1'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
    ),
    'ls7_level1_scene': SceneCollection(
        query={'product': 'ls7_level1_scene'},
        base_path=Path('/g/data/v10/reprocess/ls7/level1'),
        offset_pattern="[0.-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
    ),
    'ls5_level1_scene': SceneCollection(
        query={'product': 'ls5_level1_scene'},
        base_path=Path('/g/data/v10/reprocess/ls5/level1'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml",
    ),
})

# NBAR & NBART Scenes:
# /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/08/output/nbar/
#           LS7_ETM_NBAR_P54_GANBAR01-002_089_078_20040816/ga-metadata.yaml
# /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/07/output/nbart/
#           LS7_ETM_NBART_P54_GANBART01-002_114_078_20040731/ga-metadata.yaml
NCI_COLLECTIONS.update({
    'ls5_nbar_scene': SceneCollection(
        query={'product': ['ls5_nbar_scene', 'ls5_nbart_scene']},
        base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls5'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
    ),
    'ls7_nbar_scene': SceneCollection(
        query={'product': ['ls7_nbar_scene', 'ls7_nbart_scene']},
        base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls7'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
    ),
    'ls8_nbar_scene': SceneCollection(
        query={'product': ['ls8_nbar_scene', 'ls8_nbart_scene']},
        base_path=Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls8'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/nbar*/LS*/ga-metadata.yaml",
    ),
})

# PQ Scenes
# /g/data/rs0/scenes/pq-scenes-tmp/ls7/2005/01/output/pqa/
#           LS7_ETM_PQ_P55_GAPQ01-002_108_075_20050113/ga-metadata.yaml
NCI_COLLECTIONS.update({
    'ls5_pq_scene': SceneCollection(
        query={'product': 'ls5_pq_scene'},
        base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls5'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
    ),
    'ls7_pq_scene': SceneCollection(
        query={'product': 'ls7_pq_scene'},
        base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls7'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
    ),
    'ls8_pq_scene': SceneCollection(
        query={'product': 'ls8_pq_scene'},
        base_path=Path('/g/data/rs0/scenes/pq-scenes-tmp/ls8'),
        offset_pattern="[0-9][0-9][0-9][0-9]/[0-9][0-9]/output/pqa/LS*/ga-metadata.yaml",
    ),
})
