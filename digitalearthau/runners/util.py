import logging

from datetime import datetime
from pathlib import Path

from typing import List, Tuple

from dateutil import tz

from ..qsub import QSubLauncher, with_qsub_runner, norm_qsub_params, TaskRunner
from .model import TaskDescription, DefaultJobParameters, TaskAppState, PbsParameters
from .. import serialise, paths

_LOG = logging.getLogger(__name__)


def init_task_app(
        job_type: str,
        source_types: List[str],
        output_types: List[str],
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
        # First type is the primary...
        output_product=output_types[0],
        time=task_datetime
    )
    task_desc = TaskDescription(
        type_=job_type,
        task_dt=task_datetime,
        events_path=work_path.joinpath('events'),
        logs_path=work_path.joinpath('logs'),
        parameters=DefaultJobParameters(
            query=datacube_query_args,
            source_types=source_types,
            output_types=output_types,
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

    qsub = QSubLauncher(
        norm_qsub_params(
            dict(
                **task_desc.runtime_state.pbs_parameters._asdict(),
                **qsub_params,
                noask=True,
                # 'head' is convention for the head node. Other nodes within the job will use different names...
                stdout=task_desc.logs_path.joinpath(f'{name.lower()}-head.out.log'),
                stderr=task_desc.logs_path.joinpath(f'{name.lower()}-head.err.log'),
            )
        )
    )

    ret_code, qsub_stdout = qsub(*command)

    if not ret_code == 0:
        print(qsub_stdout)
        # The error probably went to stderr, which is not captured?
        raise RuntimeError("qsub failure. See previous error?")

    job_id = qsub_stdout.strip(' \n')
    _LOG.info('Submitted %r job: %r', name, job_id)

    return job_id
