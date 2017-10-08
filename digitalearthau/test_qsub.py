from digitalearthau.events import TaskEvent
from digitalearthau.qsub import celery_event_to_task
from . import qsub


def test_parse_args():
    p = qsub.parse_comma_args('nodes=1,mem=small')
    assert 'mem' in p
    assert 'nodes' in p
    assert p['mem'] == 'small'
    assert p['nodes'] == '1'


def test_norm_qsub_params():
    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10s')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 16
    assert p['walltime'] == '0:00:10'
    assert p['mem'] == '32256MB'

    p = qsub.parse_comma_args('nodes=1,mem=small,walltime=10m')
    p = qsub.norm_qsub_params(p)

    assert p['ncpus'] == 16
    assert p['walltime'] == '0:10:00'
    assert p['mem'] == '32256MB'

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


def test_celery_success_to_task():

    task_success_message = celery_event_to_task(
        'fc.create',
        _SUCCESS_CELERY_TASK,
        user='testuser'
    )

    assert task_success_message == TaskEvent()


_FAILED_CELERY_TASK = """
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 27204, "clock": 158, "uuid": "410e05e3-4058-4bfa-bbc2-5fc085464841", "exception": "FileExistsError(17, 'Output file already exists')", "traceback": "Traceback (most recent call last):\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/celery/app/trace.py\", line 374, in trace_task\n    R = retval = fun(*args, **kwargs)\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/celery/app/trace.py\", line 629, in __protected_call__\n    return self.run(*args, **kwargs)\n  File \"/home/jez/prog/datacube/datacube/_celery_runner.py\", line 57, in run_cloud_pickled_function\n    return func(*args, **kwargs)\n  File \"/g/data/v10/public/modules/agdc-py3-env/20170728/envs/agdc/lib/python3.6/site-packages/fc/fc_app.py\", line 144, in do_fc_task\n    raise OSError(errno.EEXIST, 'Output file already exists', str(file_path))\nFileExistsError: [Errno 17] Output file already exists: '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20151007014520000000_v1507076205.nc'\n", "timestamp": 1507182775.3487089, "type": "task-failed", "local_received": 1507182775.3507082, "state": "FAILURE"}
"""

_SUCCESS_CELERY_TASK = """
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 29517, "clock": 171, "uuid": "13d1e3c4-cecd-4306-903f-97ed1ec2d73d", "result": "<xarray.DataArray (time: 1)>\narray([ Dataset <id=437d96cb-b65d-4186-8501-18b40658bac6 type=ls8_fc_albers location=/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20150304015114500000_v1507241388.nc>], dtype=object)\nCoordinates:\n  * time     (time) datetime64[ns] 2015-03-04T01:51:14.500000", "runtime": 70.13666241300234, "timestamp": 1507241575.8904157, "type": "task-succeeded", "local_received": 1507241575.891886, "state": "SUCCESS"}
"""

_RECEIVED_CELERY_TASK = """
{"hostname": "celery@kveikur", "utcoffset": -11, "pid": 29517, "clock": 313, "uuid": "788bdede-ac9c-4edb-bde5-f26013148cf4", "name": "datacube._celery_runner.run_cloud_pickled_function", "args": "(functools.partial(<function do_fc_task at 0x7fb79d1332f0>, {'source_type': 'ls8_nbar_albers', 'output_type': 'ls8_fc_albers', 'version': '${version}', 'description': 'Landsat 8 Fractional Cover 25 metre, 100km tile, Australian Albers Equal Area projection (EPSG:3577)', 'product_type': 'fractional_cover', 'location': '/g/data/fk4/datacube/002/', 'file_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}_v{version}.nc', 'partial_ncml_path_template': 'LS8_OLI_FC/{tile_index[0]}_{tile_index[1]}/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}_{start_time}.ncml', 'ncml_path_template': 'LS8_OLI_FC/LS8_OLI_FC_3577_{tile_index[0]}_{tile_index[1]}.ncml', 'sensor_regression_coefficients': {'blue': [0.00041, 0.9747], 'green': [0.00289, 0.99779], 'red': [0.00274, 1.00446], 'nir': [4e-05, 0.98906], 'swir1': [0.00256, 0.99467], 'swir2': [-0.00327, 1.02551]}, 'global_attributes': {'title': 'Fractional Cover 25 v2', 'summary': \"The Fractional Cover (FC)...,)", "kwargs": "{'task': {'nbar': Tile<sources=<xarray.DataArray (time: 1)>\narray([ (Dataset <id=d4a9f344-7c5c-4c69-b70d-2e61c25a66a3 type=ls8_nbar_albers location=/g/data/rs0/datacube/002/LS8_OLI_NBAR/-11_-28/LS8_OLI_NBAR_3577_-11_-28_2015_v1496400956.nc>,)], dtype=object)\nCoordinates:\n  * time     (time) datetime64[ns] 2015-09-12T01:51:24,\n\tgeobox=GeoBox(4000, 4000, Affine(25.0, 0.0, -1100000.0,\n       0.0, -25.0, -2700000.0), EPSG:3577)>, 'tile_index': (-11, -28, numpy.datetime64('2015-09-12T01:51:24.000000000')), 'filename': '/g/data/fk4/datacube/002/LS8_OLI_FC/-11_-28/LS8_OLI_FC_3577_-11_-28_20150912015124000000_v1507241388.nc'}}", "root_id": "788bdede-ac9c-4edb-bde5-f26013148cf4", "parent_id": null, "retries": 0, "eta": null, "expires": null, "timestamp": 1507241710.5862098, "type": "task-received", "local_received": 1507241710.5875244, "state": "RECEIVED"}
"""
