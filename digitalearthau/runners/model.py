from pathlib import Path

import datetime
from typing import NamedTuple


class TaskAppParameters(NamedTuple):
    """
    Parameters for apps using the task_app framework.
    """
    # Input app config path
    config_path: Path
    # Path where tasks are stored, once calculated
    task_serialisation_path: Path


class TaskDescription(NamedTuple):
    # task type (eg. "fc")
    type_: str
    # The submission timestamp used for all files/directories produced in the job.
    task_dt: datetime.datetime

    # Directory of event log outputs
    events_path: Path
    # Directory of plain-text log outputs
    logs_path: Path
    # Datacube query args used to select datasets to process (eg time=(1994, 1995), product=ls5_nbar_albers)
    query: dict

    # Parameters specific to the task type.
    # (Expect this type to be a union eventually: other app types might be added in the future...)
    parameters: TaskAppParameters
