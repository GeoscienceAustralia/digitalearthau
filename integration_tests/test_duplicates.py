import uuid
from pathlib import Path

import pytest
from typing import Tuple

from datacube.index._api import Index
from digitalearthau import duplicates
from digitalearthau.index import AgdcDatasetPathIndex, DatasetLite

import click.testing

_EXPECTED_ALL_DUPLICATES = """product,time_lower_day,platform,count,dataset_refs
ls8_level1_scene,2016-09-26T00:00:00+00:00,114,80,2,86150afc-b7d5-4938-a75e-3445007256d3\
 f882f9c0-a27f-11e7-a89f-185e0f80a5c0
"""
_EXPECTED_SPECIFIC_DUPS = """product,time_lower_day,sat_path_lower,sat_row_lower,count,dataset_refs
ls8_level1_scene,2016-09-26T00:00:00+00:00,114,80,2,86150afc-b7d5-4938-a75e-3445007256d3\
 f882f9c0-a27f-11e7-a89f-185e0f80a5c0
"""
ON_DISK1_ID = uuid.UUID('86150afc-b7d5-4938-a75e-3445007256d3')
ON_DISK2_ID = uuid.UUID('10c4a9fe-2890-11e6-8ec8-a0000100fe80')

ON_DISK1_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20160926', 'ga-metadata.yaml')
ON_DISK2_OFFSET = ('LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20150924', 'ga-metadata.yaml')

# Source datasets that will be indexed if on_disk1 is indexed
ON_DISK1_PARENT = uuid.UUID('dee471ed-5aa5-46f5-96b5-1e1ea91ffee4')

ON_DISK1_DUP_ID = uuid.UUID("f882f9c0-a27f-11e7-a89f-185e0f80a5c0")


@pytest.fixture
def indexed_ls8_l1_scenes(dea_index: Index, integration_test_data: Path) -> Tuple[uuid.UUID, uuid.UUID]:
    ds1 = integration_test_data.joinpath(*ON_DISK1_OFFSET)
    ds2 = integration_test_data.joinpath(*ON_DISK2_OFFSET)

    AgdcDatasetPathIndex(dea_index).add_dataset(DatasetLite(ON_DISK1_ID), ds1.as_uri())
    AgdcDatasetPathIndex(dea_index).add_dataset(DatasetLite(ON_DISK2_ID), ds2.as_uri())

    return ON_DISK1_ID, ON_DISK2_ID


@pytest.fixture
def duplicate_ls8_l1_scene(dea_index: Index, integration_test_data: Path) -> uuid.UUID:
    dd1 = integration_test_data.joinpath(
        'dupe', 'LS8_OLITIRS_OTH_P51_GALPGS01-032_114_080_20160926_2', 'ga-metadata.yaml'
    )

    AgdcDatasetPathIndex(dea_index).add_dataset(DatasetLite(ON_DISK1_DUP_ID), dd1.as_uri())
    return ON_DISK1_DUP_ID


def test_no_duplicates(global_integration_cli_args,
                       indexed_ls8_l1_scenes: Tuple[uuid.UUID, uuid.UUID]):
    res = _run_cmd(['-a'], global_integration_cli_args)
    # Just the headers, no results
    assert res.output == 'product,time_lower_day,platform,count,dataset_refs\n'
    res = _run_cmd(['ls8_level1_scene'], global_integration_cli_args)
    assert res.output == 'product,time_lower_day,sat_path_lower,sat_row_lower,count,dataset_refs\n'


def test_duplicates(global_integration_cli_args,
                    indexed_ls8_l1_scenes: Tuple[uuid.UUID, uuid.UUID], duplicate_ls8_l1_scene: uuid.UUID):
    res = _run_cmd(['--all_'], global_integration_cli_args)
    assert res.output == _EXPECTED_ALL_DUPLICATES

    res = _run_cmd(['ls8_level1_scene'], global_integration_cli_args)
    assert res.output == _EXPECTED_SPECIFIC_DUPS


def _run_cmd(args, global_integration_cli_args) -> click.testing.Result:
    res = click.testing.CliRunner().invoke(
        duplicates.cli, args=[
            *global_integration_cli_args,
            *args
        ],
        catch_exceptions=False
    )
    return res
