from io import StringIO
from uuid import UUID

import os

import pytest
from boltons.jsonutils import JSONLIterator
from datetime import datetime

from dateutil import tz
from unittest import mock
from typing import List

from digitalearthau.events import TaskEvent, NodeMessage, Status
from digitalearthau.runners import model
from digitalearthau.runners.celery_environment import _celery_event_to_task
from . import qsub

import celery.events.state as celery_state

from datacube import _celery_runner as cr

# Flake8 doesn't allow ignoring just one error for a whole file,
# so because of the long lines in here, lets ignore all errors in the file.
# Hey, it's only a linter...
# flake8: noqa

###############################################
# Test Utilities re PBS Job Submission
###############################################


def test_parse_args():
    p = qsub.parse_comma_args('nodes=1,mem=small')
    assert 'mem' in p
    assert 'nodes' in p
    assert p['mem'] == 'small'
    assert p['nodes'] == '1'


def test_norm_qsub_params():
    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10s')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 48
    assert p['walltime'] == '0:00:10'
    assert p['mem'] == '97280MB'

    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10m,extra_qsub_args=-M test@email.com.au -m ae')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 48
    assert p['walltime'] == '0:10:00'
    assert p['mem'] == '97280MB'
    assert p['extra_qsub_args'] == ['-M', 'test@email.com.au', '-m', 'ae']

    p = qsub.parse_comma_args('ncpus=1, mem=medium, walltime=3h')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 1
    assert p['walltime'] == '3:00:00'
    assert p['mem'] == '4096MB'


def test_remove_args():
    args1 = '--qsub 10 --foo bar'.split(' ')
    args2 = '--qsub=10 --foo bar'.split(' ')
    args3 = '--removed --foo bar'.split(' ')

    assert qsub.remove_args('--qsub', args1, 1) == ['--foo', 'bar']
    assert qsub.remove_args('--qsub', args2, 1) == ['--foo', 'bar']
    assert qsub.remove_args('--removed', args3, 0) == ['--foo', 'bar']


#################################################
# Tests of Celery Executor Related Functionality
#################################################

# Three events: received, started, success
_SUCCESS_CELERY_EVENTS = r"""
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 29517, "clock": 3, "uuid": "13d1e3c4-cecd-4306-903f-97ed1ec2d73d", "name": "datacube._celery_runner.run_cloud_pickled_function", "args": "(functools.partial(<function do_fc_task at 0x7fb79d1332f0>, {'source_type': 'ls8_nbar_albers', 'output_type': 'ls8_fc_albers', 'version': '${version}', 'description': 'Landsat 8 Fractional Cover 25 metre, 100km tile, Australian Albers Equal Area projection (EPSG:3577)', 'product_type': 'fractional_cover', 'location': '/g/data/fk4/datacube/002/', 'file_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}_v{version}.nc', 'partial_ncml_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.ncml', 'ncml_path_template': 'LS8_OLI_FC/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}.ncml', 'sensor_regression_coefficients': {'blue': [0.00041, 0.9747], 'green': [0.00289, 0.99779], 'red': [0.00274, 1.00446], 'nir': [4e-05, 0.98906], 'swir1': [0.00256, 0.99467], 'swir2': [-0.00327, 1.02551]}, 'global_attributes': {'title': 'Fractional Cover 25 v2', 'summary': \"The Fractional Cover (FC)...,)", "kwargs": "{'task': {'nbar': Tile<sources=<xarray.DataArray (time: 1)>\narray([ (Dataset <id=60bc52f1-7a70-43f2-bc8d-2bd138eb2aba type=ls8_nbar_albers location=/g/data/rs0/datacube/002/LS8_OLI_NBAR/-11_-28/LS8_OLI_NBAR_3577_-11_-28_2015_v1496400956.nc>,)], dtype=object)\nCoordinates:\n  * time     (time) datetime64[ns] 2015-03-04T01:51:14.500000,\n\tgeobox=GeoBox(4000, 4000, Affine(25.0, 0.0, -1100000.0,\n       0.0, -25.0, -2700000.0), EPSG:3577)>, 'tile_index': (-11, -28, numpy.datetime64('2015-03-04T01:51:14.500000000')), 'filename': '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20150304015114500000_v1507241388.nc'}}", "root_id": "13d1e3c4-cecd-4306-903f-97ed1ec2d73d", "parent_id": null, "retries": 0, "eta": null, "expires": null, "timestamp": 1507241402.9484067, "type": "task-received", "local_received": 1507241402.9497962, "state": "RECEIVED"}
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 29517, "clock": 99, "uuid": "13d1e3c4-cecd-4306-903f-97ed1ec2d73d", "timestamp": 1507241505.7179525, "type": "task-started", "local_received": 1507241505.7221746, "state": "STARTED"}
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 29517, "clock": 171, "uuid": "13d1e3c4-cecd-4306-903f-97ed1ec2d73d", "result": "<xarray.DataArray (time: 1)>\narray([ Dataset <id=437d96cb-b65d-4186-8501-18b40658bac6 type=ls8_fc_albers location=/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20150304015114500000_v1507241388.nc>], dtype=object)\nCoordinates:\n  * time     (time) datetime64[ns] 2015-03-04T01:51:14.500000", "runtime": 70.13666241300234, "timestamp": 1507241575.8904157, "type": "task-succeeded", "local_received": 1507241575.891886, "state": "SUCCESS"}
"""

# Three events produced: pending, active, success
_EXPECTED_SUCCESS = [
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 22, 10, 2, 948407, tzinfo=tz.tzutc()),
        event='task.pending',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message=None,
        id='13d1e3c4-cecd-4306-903f-97ed1ec2d73d',
        status=Status.PENDING,
        name='fc.test',
        input_datasets=(UUID('60bc52f1-7a70-43f2-bc8d-2bd138eb2aba'),),
        output_datasets=None,
        job_parameters={},
        # All parent_ids are calculated from the below "@mock.patch.dict(os.environ, {'PBS_JOBID': '87654321.gadi-pbs'})"
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')),
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 22, 11, 45, 717952, tzinfo=tz.tzutc()),
        event='task.active',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message=None,
        id='13d1e3c4-cecd-4306-903f-97ed1ec2d73d',
        status=Status.ACTIVE,
        name='fc.test',
        input_datasets=(UUID('60bc52f1-7a70-43f2-bc8d-2bd138eb2aba'),),
        output_datasets=None,
        job_parameters={},
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')
    ),
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 22, 12, 55, 890416, tzinfo=tz.tzutc()),
        event='task.complete',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message=None,
        id='13d1e3c4-cecd-4306-903f-97ed1ec2d73d',
        status=Status.COMPLETE,
        name='fc.test',
        input_datasets=(UUID('60bc52f1-7a70-43f2-bc8d-2bd138eb2aba'),),
        output_datasets=None,
        job_parameters={},
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')
    )
]

# JSONL format needs one per line
# pylint: disable=line-too-long
_FAIL_CELERY_EVENTS = r"""
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 27204, "clock": 156, "uuid": "410e05e3-4058-4bfa-bbc2-5fc085464841", "name": "datacube._celery_runner.run_cloud_pickled_function", "args": "(functools.partial(<function do_fc_task at 0x7f4dd49ff2f0>, {'source_type': 'ls8_nbar_albers', 'output_type': 'ls8_fc_albers', 'version': '${version}', 'description': 'Landsat 8 Fractional Cover 25 metre, 100km tile, Australian Albers Equal Area projection (EPSG:3577)', 'product_type': 'fractional_cover', 'location': '/g/data/fk4/datacube/002/', 'file_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}_v{version}.nc', 'partial_ncml_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.ncml', 'ncml_path_template': 'LS8_OLI_FC/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}.ncml', 'sensor_regression_coefficients': {'blue': [0.00041, 0.9747], 'green': [0.00289, 0.99779], 'red': [0.00274, 1.00446], 'nir': [4e-05, 0.98906], 'swir1': [0.00256, 0.99467], 'swir2': [-0.00327, 1.02551]}, 'global_attributes': {'title': 'Fractional Cover 25 v2', 'summary': \"The Fractional Cover (FC)...,)", "kwargs": "{'task': {'nbar': Tile<sources=<xarray.DataArray (time: 1)>\narray([ (Dataset <id=591fce1d-5268-44e8-a8b0-e38e6cfbb749 type=ls8_nbar_albers location=/g/data/rs0/datacube/002/LS8_OLI_NBAR/-11_-28/LS8_OLI_NBAR_3577_-11_-28_2015_v1496400956.nc>,)], dtype=object)\nCoordinates:\n  * time     (time) datetime64[ns] 2015-10-07T01:45:20,\n\tgeobox=GeoBox(4000, 4000, Affine(25.0, 0.0, -1100000.0,\n       0.0, -25.0, -2700000.0), EPSG:3577)>, 'tile_index': (-11, -28, numpy.datetime64('2015-10-07T01:45:20.000000000')), 'filename': '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20151007014520000000_v1507076205.nc'}}", "root_id": "410e05e3-4058-4bfa-bbc2-5fc085464841", "parent_id": null, "retries": 0, "eta": null, "expires": null, "timestamp": 1507182775.3364704, "type": "task-received", "local_received": 1507182775.337901, "state": "RECEIVED"}
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 27204, "clock": 157, "uuid": "410e05e3-4058-4bfa-bbc2-5fc085464841", "timestamp": 1507182775.3381906, "type": "task-started", "local_received": 1507182775.3400292, "state": "STARTED"}
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 27204, "clock": 158, "uuid": "410e05e3-4058-4bfa-bbc2-5fc085464841", "exception": "FileExistsError(17, 'Output file already exists')", "traceback": "Traceback (most recent call last):\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/celery/app/trace.py\", line 374, in trace_task\n    R = retval = fun(*args, **kwargs)\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/celery/app/trace.py\", line 629, in __protected_call__\n    return self.run(*args, **kwargs)\n  File \"/home/jez/prog/datacube/datacube/_celery_runner.py\", line 57, in run_cloud_pickled_function\n    return func(*args, **kwargs)\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/fc/fc_app.py\", line 144, in do_fc_task\n    raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))\nFileExistsError: [Errno 17] Output file already exists: '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20151007014520000000_v1507076205.nc'\n", "timestamp": 1507182775.3487089, "type": "task-failed", "local_received": 1507182775.3507082, "state": "FAILURE"}
"""
# Three events produced: pending, active, success
_EXPECTED_FAILURE = [
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 5, 52, 55, 336470, tzinfo=tz.tzutc()),
        event='task.pending',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message=None,
        id='410e05e3-4058-4bfa-bbc2-5fc085464841',
        status=Status.PENDING,
        name='fc.test',
        input_datasets=(UUID('591fce1d-5268-44e8-a8b0-e38e6cfbb749'),),
        output_datasets=None,
        job_parameters={},
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')
    ),
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 5, 52, 55, 338191, tzinfo=tz.tzutc()),
        event='task.active',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message=None,
        id='410e05e3-4058-4bfa-bbc2-5fc085464841',
        status=Status.ACTIVE,
        name='fc.test',
        input_datasets=(UUID('591fce1d-5268-44e8-a8b0-e38e6cfbb749'),),
        output_datasets=None,
        job_parameters={},
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')
    ),
    TaskEvent(
        timestamp=datetime(2017, 10, 5, 5, 52, 55, 348709, tzinfo=tz.tzutc()),
        event='task.failed',
        user='testuser',
        node=NodeMessage(
            hostname='kveikur',
            pid=None,
            runtime_id=None
        ),
        message='Traceback (most recent call last):\n  File "/g/data/v10/public/modules/agdc-py3-env/20170728/envs/'
                'agdc/lib/python3.6/site-packages/celery/app/trace.py", line 374, in trace_task\n'
                '    R = retval = fun(*args, **kwargs)\n'
                '  File "/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/'
                'site-packages/celery/app/trace.py", line 629, in __protected_call__\n'
                '    return self.run(*args, **kwargs)\n'
                '  File "/home/jez/prog/datacube/datacube/_celery_runner.py", line 57, in run_cloud_pickled_function\n'
                '    return func(*args, **kwargs)\n'
                '  File "/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/'
                'site-packages/fc/fc_app.py", line 144, in do_fc_task\n'
                '    raise OSError(errno.EEXIST, \'Output file already exists\', str(file_path))\n'
                'FileExistsError: [Errno 17] Output file already exists: '
                '\'/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_'
                '20151007014520000000_v1507076205.nc\'\n',
        id='410e05e3-4058-4bfa-bbc2-5fc085464841',
        status=Status.FAILED,
        name='fc.test',
        input_datasets=(UUID('591fce1d-5268-44e8-a8b0-e38e6cfbb749'),),
        output_datasets=None,
        job_parameters={},
        parent_id=UUID('6c5e209a-6d56-5460-9a30-20e264492d5c')
    )
]


@pytest.mark.parametrize("input_json,expected_events", [
    (_SUCCESS_CELERY_EVENTS, _EXPECTED_SUCCESS),
    (_FAIL_CELERY_EVENTS, _EXPECTED_FAILURE),
])
@mock.patch.dict(os.environ, {'PBS_JOBID': '87654321.gadi-pbs'})
def test_celery_success_to_task(input_json: str, expected_events: List[TaskEvent]):
    state: celery_state.State = cr.app.events.State()

    task_id = expected_events[0].id

    task_description = model.TaskDescription(
        type_="fc.test",
        task_dt=None,
        events_path=None,
        logs_path=None,
        parameters={},
        # Task-app framework
        runtime_state=model.TaskAppState(
            config_path=None,
            task_serialisation_path=None,
        )
    )

    events = []
    for j in JSONLIterator(StringIO(input_json)):
        state.event(j)

        celery_task: celery_state.Task = state.tasks[task_id]
        events.append(_celery_event_to_task(
            task_description,
            celery_task,
            user='testuser'
        ))

    # test events one at a time as the pytest failure output is more readable...
    assert len(events) == len(expected_events)
    for i, event in enumerate(events):
        assert event == expected_events[i]
