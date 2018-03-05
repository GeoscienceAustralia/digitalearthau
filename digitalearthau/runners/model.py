"""
Model objects for tracking the state and outputs of a task in NCI,
such as log locations, pbs parameters etc.
"""
from pathlib import Path

import datetime
from typing import NamedTuple, List, Tuple


class PbsParameters(NamedTuple):
    """
    PBS-running context: options to be reused if a task needs to submit further tasks

    (Any args specific to the task being run are excluded: CPU/memory/nodes
     because any subtasks have different values...)
    """
    # Args names match qsub.VALID_KEYS names

    project: str
    queue: str

    # Envronment variables to set
    env_vars: dict = {}

    # Default group and world read
    umask: int = 33

    # Addition raw cli arguments to append to qsub commands. Be careful!
    extra_qsub_args: List[str] = []


class TaskAppState(NamedTuple):
    """
    Common state for apps using the task_app framework.
    """
    # Input app config path
    config_path: Path
    # Path where tasks are stored, once calculated
    task_serialisation_path: Path

    pbs_parameters: PbsParameters = None


class DefaultJobParameters(NamedTuple):
    """
    Input ("user") parameters for the job.

    These will often be stored or shown as a way to distinguish the task.
    """
    # Datacube query args used to select datasets to process (eg time=(1994, 1995), product=ls5_nbar_albers)
    query: dict

    # Input and output product names ("ls7_nbar_albers")
    source_products: List[str]
    output_products: List[str]


class TaskDescription(NamedTuple):
    """
    Representation of a task that has been submitted

    It contains information useful to jobs: such as log locations, how to create
    subtasks, information about the job for provenance etc.

    It could correspond to several pbs jobs.
    """
    # task type (eg. "fc")
    type_: str
    # The submission timestamp used for all files/directories produced in the job.
    task_dt: datetime.datetime

    # Directory of event log outputs
    events_path: Path
    # Directory of stdout/stderr log outputs
    logs_path: Path

    # Parameters that are unique to this job, such as the datacube query used to find datasets.
    # (Expect this type to be a union or eventually: other apps may have different parameters?
    #  but we'd prefer to keep them standardised if possible..)
    parameters: DefaultJobParameters

    # Parameters specific to the task runtime (eg. datacube task_app).
    # (Expect this type to be a union eventually: other runtime types might be added in the future...)
    runtime_state: TaskAppState = None

    # Folder containing records of submitted jobs.
    jobs_path: Path = None
