import getpass
import json
import os
import socket
import uuid
import datetime
from typing import NamedTuple, Optional, List

from enum import Enum, unique

import pathlib

from digitalearthau import pbs


class NodeMessage(NamedTuple):
    hostname: str
    pid: int

    # An optional id to identify a single instance within a host, such as a worker. (pid is not unique)
    runtime_id: Optional[uuid.UUID] = None

    def current_node(self) -> 'NodeMessage':
        return NodeMessage(
            hostname=_CURRENT_HOST,
            # "Computed" every time in case of forking.
            pid=os.getpid()
        )


_CURRENT_HOST = socket.getfqdn()


class BaseMessage(NamedTuple):
    timestamp: datetime.datetime

    # Name of event
    # By convention, lowercase alphanumeric separated by dots
    # Eg. 'dataset.created'
    #     'task.failed'
    event: str

    user: Optional[str]

    node: NodeMessage

    # Human-readable string for the event
    # eg. "Archived due to corruption"
    message: Optional[str] = None


@unique
class Status(Enum):
    # Not yet received or queued: to-pre-announce an expected future task, such as on reception of a future satellite
    # schedule.
    SCHEDULED = 1
    # Received but can't be processed yet: such as waiting for Ancillary data.
    WAITING = 2
    # Queued to run
    PENDING = 3
    # .... the rest are self-expanatory
    ACTIVE = 4
    COMPLETE = 5
    FAILED = 6
    CANCELLED = 7


class TaskEvent(NamedTuple):
    ################
    # Base fields (common to all events)

    # UTC timestamp
    timestamp: datetime.datetime

    # Name of event
    # By convention, lowercase alphanumeric separated by dots
    # Eg. 'dataset.created'
    #     'task.failed'
    event: str

    # User who triggered the event (eg. archived the dataset)
    user: Optional[str]

    node: NodeMessage

    # Human-readable string for the event
    # eg. "Archived due to corruption"
    message: Optional[str]

    ################
    # Task fields

    # ID of this task
    # 'id' is already taken in python. TODO: can our serializer support stripping a trailing underscore?
    # pylint: disable=invalid-name
    id: uuid.UUID

    status: Status
    # Name of this kind of task.
    # Eg. "galpgs.create".
    # (properties like time range go in the job_parameters field below, not in this name)
    name: str

    # Input/ouytput datasets if known
    input_datasets: Optional[List[uuid.UUID]] = None
    output_datasets: Optional[List[uuid.UUID]] = None

    # Parameters for this job (eg, datacube_query_args)
    # Note that this default value is mutable: TODO it may be initialised with values at the start of the job?
    job_parameters: dict = {}

    # Most tasks created here will be children of the PBS job (task) they run in.
    parent_id: uuid.UUID = pbs.current_job_task_id()

