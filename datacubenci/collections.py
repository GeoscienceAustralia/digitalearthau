# coding=utf-8

"""
A collection is this case is datacube-query-arguments and a folder-on-disk
that should contain the same set of datasets.

(Our sync script will compare/"sync" the two)
"""

from pathlib import Path
from typing import Iterable


class Collection:
    def __init__(self,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 unique_fields: Iterable[str],
                 delete_archived_after_days=None):
        self.query = query
        self.base_path = base_path
        self.offset_pattern = offset_pattern
        self.unique_fields = unique_fields
        self.delete_archived_after_days = delete_archived_after_days


class SceneCollection(Collection):
    def __init__(self,
                 query: dict,
                 base_path: Path,
                 offset_pattern: str,
                 delete_archived_after_days=None):
        super().__init__(query, base_path, offset_pattern,
                         ('sat_path.lower', 'sat_row.lower', 'time.lower.day'),
                         delete_archived_after_days)


_YEAR_MONTH_GL = "[0-9][0-9][0-9][0-9]/[0-9][0-9]"

NCI_COLLECTIONS = {
    'telemetry': Collection(
        {'metadata_type': 'telemetry'},
        Path('/g/data/v10/repackaged/rawdata/0'),
        _YEAR_MONTH_GL + "/*/ga-metadata.yaml",
        ('platform', 'time.lower.day')
    ),
}

# Level 1
# /g/data/v10/reprocess/ls7/level1/2016/06/
#           LS7_ETM_SYS_P31_GALPGS01-002_103_074_20160617/ga-metadata.yaml
_LEVEL1_GL = _YEAR_MONTH_GL + "/LS*/ga-metadata.yaml"
NCI_COLLECTIONS.update({
    'ls8_level1_scene': SceneCollection(
        {'product': ['ls8_level1_scene', 'ls8_level1_oli_scene']},
        Path('/g/data/v10/reprocess/ls8/level1'),
        _LEVEL1_GL
    ),
    'ls7_level1_scene': SceneCollection(
        {'product': 'ls7_level1_scene'},
        Path('/g/data/v10/reprocess/ls7/level1'),
        _LEVEL1_GL
    ),
    'ls5_level1_scene': SceneCollection(
        {'product': 'ls5_level1_scene'},
        Path('/g/data/v10/reprocess/ls5/level1'),
        _LEVEL1_GL
    ),
})

# NBAR & NBART Scenes:
# /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/08/output/nbar/
#           LS7_ETM_NBAR_P54_GANBAR01-002_089_078_20040816/ga-metadata.yaml
# /g/data/rs0/scenes/nbar-scenes-tmp/ls7/2004/07/output/nbart/
#           LS7_ETM_NBART_P54_GANBART01-002_114_078_20040731/ga-metadata.yaml
_NBAR_GL = _YEAR_MONTH_GL + "/output/nbar*/LS*/ga-metadata.yaml"
NCI_COLLECTIONS.update({
    'ls5_nbar_scene': SceneCollection(
        {'product': ['ls5_nbar_scene', 'ls5_nbart_scene']},
        Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls5'),
        _NBAR_GL
    ),
    'ls7_nbar_scene': SceneCollection(
        {'product': ['ls7_nbar_scene', 'ls7_nbart_scene']},
        Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls7'),
        _NBAR_GL
    ),
    'ls8_nbar_scene': SceneCollection(
        {'product': ['ls8_nbar_scene', 'ls8_nbart_scene']},
        Path('/g/data/rs0/scenes/nbar-scenes-tmp/ls8'),
        _NBAR_GL
    ),
})

# PQ Scenes
# /g/data/rs0/scenes/pq-scenes-tmp/ls7/2005/01/output/pqa/
#           LS7_ETM_PQ_P55_GAPQ01-002_108_075_20050113/ga-metadata.yaml
_PQA_GL = _YEAR_MONTH_GL + "/output/pqa/LS*/ga-metadata.yaml"
NCI_COLLECTIONS.update({
    'ls5_pq_scene': SceneCollection(
        {'product': 'ls5_pq_scene'},
        Path('/g/data/rs0/scenes/pq-scenes-tmp/ls5'),
        _PQA_GL
    ),
    'ls7_pq_scene': SceneCollection(
        {'product': 'ls7_pq_scene'},
        Path('/g/data/rs0/scenes/pq-scenes-tmp/ls7'),
        _PQA_GL
    ),
    'ls8_pq_scene': SceneCollection(
        {'product': 'ls8_pq_scene'},
        Path('/g/data/rs0/scenes/pq-scenes-tmp/ls8'),
        _PQA_GL
    ),
})
