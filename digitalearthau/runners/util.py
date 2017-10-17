import getpass
import logging
import shlex
import socket

from datetime import datetime
from pathlib import Path

from typing import List, Tuple

import yaml
from dateutil import tz
from digitalearthau import pbs

from ..qsub import QSubLauncher, with_qsub_runner, norm_qsub_params, TaskRunner
from .model import TaskDescription, DefaultJobParameters, TaskAppState, PbsParameters
from .. import serialise, paths

_LOG = logging.getLogger(__name__)


def init_task_app(
        job_type: str,
        source_products: List[str],
        output_products: List[str],
        datacube_query_args: dict,
        app_config_path: Path,
        pbs_project: str,
        pbs_queue: str) -> Tuple[TaskDescription, Path]:
    """
    Convenience function for creating and writing a task description
    for an app using the task_app framework

    Creates a work directory and sets up the common folder structure.
    """
    task_datetime = datetime.utcnow().replace(tzinfo=tz.tzutc())
    work_path = paths.get_product_work_directory(
        # First is the primary...
        output_product=output_products[0],
        time=task_datetime
    )
    _LOG.info("Created work directory %s", work_path)

    task_desc = TaskDescription(
        type_=job_type,
        task_dt=task_datetime,
        events_path=work_path.joinpath('events'),
        logs_path=work_path.joinpath('logs'),
        jobs_path=work_path.joinpath('jobs'),
        parameters=DefaultJobParameters(
            query=datacube_query_args,
            source_products=source_products,
            output_products=output_products,
        ),
        # Task-app framework
        runtime_state=TaskAppState(
            config_path=app_config_path,
            task_serialisation_path=work_path.joinpath('generated-tasks.pickle'),
            pbs_parameters=PbsParameters(
                project=pbs_project,
                queue=pbs_queue,
            )
        ),
    )
    task_desc.logs_path.mkdir(parents=True, exist_ok=False)
    task_desc.events_path.mkdir(parents=True, exist_ok=False)
    task_desc.jobs_path.mkdir(parents=True, exist_ok=False)

    task_desc_path = work_path.joinpath('task-description.json')
    serialise.dump_structure(task_desc_path, task_desc)
    return task_desc, task_desc_path


def submit_subjob(
        name: str,
        task_desc: TaskDescription,
        command: List[str],
        qsub_params) -> str:
    """
    Convenience method for submitting a sub job under the given task_desc

    It will set up output locations and pbs parameters for you.

    Sub-job name should be unique for the task_desc.
    """
    if not name.isidentifier():
        raise ValueError("sub-job name must be alphanumeric, eg 'generate', 'run_2013")

    submit_time = datetime.utcnow()
    timestamp = submit_time.timestamp()

    # 'head' is convention for the head node. Other nodes within the job will use different names...
    stdout_path = task_desc.logs_path.joinpath(f'{int(timestamp)}-{name.lower()}-head.out.log')
    stderr_path = task_desc.logs_path.joinpath(f'{int(timestamp)}-{name.lower()}-head.err.log')
    qsub = QSubLauncher(
        norm_qsub_params(
            dict(
                **task_desc.runtime_state.pbs_parameters._asdict(),
                **qsub_params,
                noask=True,
                stdout=stdout_path,
                stderr=stderr_path,
            )
        )
    )

    args, script = qsub.build_submission(*command)

    job_id = qsub.submit(*command)
    _LOG.info('Submitted %r: %s', name, job_id)

    # Record the job information in the jobs directory.
    submitter_info = dict(
        user=getpass.getuser(),
        hostname=socket.gethostname(),
    )
    if pbs.is_under_pbs():
        submitter_info['job_id'] = pbs.current_pbs_job_id()

    # This is yaml because the multiline text fields are far easier to read.
    submission_info_path = task_desc.jobs_path.joinpath(f'{int(timestamp)}-{name.lower()}-{job_id}.yaml')
    submission_info_path.write_text(
        yaml.dump(
            dict(
                name=name,
                submit_dt=submit_time,
                command=_str_command_args(command),
                pbs=dict(
                    qsub=dict(
                        arguments=_str_command_args(args),
                        script=serialise.MultilineString(script),
                    ),
                    job_id=job_id,
                ),
                logs=dict(
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                ),
                submitter=submitter_info
            ),
            default_flow_style=False,
            indent=4,
        )
    )
    _LOG.info("Submission info: %s", submission_info_path)

    return job_id


def _str_command_args(args):
    return ' '.join(shlex.quote(arg) for arg in args)
