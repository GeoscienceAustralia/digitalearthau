from pathlib import Path

import datetime
from typing import NamedTuple, List


class TaskAppState(NamedTuple):
    """
    Parameters for apps using the task_app framework.
    """
    # Input app config path
    config_path: Path
    # Path where tasks are stored, once calculated
    task_serialisation_path: Path


class DefaultJobParameters(NamedTuple):
    # Datacube query args used to select datasets to process (eg time=(1994, 1995), product=ls5_nbar_albers)
    query: dict

    # Input and output products ("ls7_nbar_albers")
    source_types: List[str]
    output_types: List[str]


class TaskDescription(NamedTuple):
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
    runtime_state: TaskAppState
