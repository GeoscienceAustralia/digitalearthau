import pathlib

import datetime
import pytest
from typing import NamedTuple, List

from digitalearthau import serialise
from digitalearthau.events import Status
from digitalearthau.runners.model import TaskDescription, DefaultJobParameters, TaskAppState
from pathlib import Path


def test_roundtrip():
    # Convert to dict and back again, checking that it's identical.

    class MyEmbeddedNamedTuple(NamedTuple):
        arg1: str
        my_dt: datetime.datetime

    class MyNamedTuple(NamedTuple):
        var1: str
        var2: List[int]
        inner_tuple: MyEmbeddedNamedTuple
        my_path: pathlib.Path
        my_enum: Status

    m = MyNamedTuple(
        var1="a string",
        var2=[1, 2, 3],
        inner_tuple=MyEmbeddedNamedTuple(
            arg1="b string",
            my_dt=datetime.datetime.utcnow()
        ),
        my_path=pathlib.Path("/tmp/test"),
        my_enum=Status.ACTIVE
    )

    d = serialise.type_to_dict(m)
    print(repr(d))
    new_m = serialise.dict_to_type(d, MyNamedTuple)
    print(repr(new_m))
    assert m == new_m


def test_dump_load_task_structure(tmpdir):
    # Dump to json and reload, check equality.

    d = Path(str(tmpdir))
    task_description = TaskDescription(
        type_="reproject",
        task_dt=datetime.datetime.utcnow(),
        events_path=d.joinpath('events'),
        logs_path=d.joinpath('logs'),
        parameters=DefaultJobParameters(
            query={'time': [2013, 2015]},
            source_products=['ls5_nbar_albers'],
            output_products=['ls5_nbar_waterman_butterfly'],
        ),
        # Task-app framework
        runtime_state=TaskAppState(
            config_path=Path('config.test.yaml'),
            task_serialisation_path=d.joinpath('generated-tasks.pickle'),
        )
    )

    serialised_file = d.joinpath('task_description.json')
    serialise.dump_structure(serialised_file, task_description)

    result = serialise.load_structure(serialised_file, expected_type=TaskDescription)

    assert result == task_description
