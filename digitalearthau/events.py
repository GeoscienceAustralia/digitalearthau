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


class LogMessage(BaseMessage):
    logger: str
    task_id: uuid.UUID

    # @classmethod
    # def now(cls, level: str, message: str, user=getpass.getuser()) -> 'LogMessage':
    #     return LogMessage(
    #         timestamp=datetime.datetime.utcnow(),
    #         event=f"log.{level.lower()}",
    #         user=user,
    #         logger='',
    #         task_id=uuid.uuid4(),
    #         message=message,
    #         node=NodeMessage(
    #             hostname=_CURRENT_HOST,
    #             # "Computed" every time in case of forking.
    #             pid=os.getpid()
    #         )
    #     )


def named_tuple_to_dict(o):
    """
    Convert a named tuple and all of it's embedded named tuples to dicts
    """
    # Convert any child NamedTuples to DICTS
    values = (named_tuple_to_dict(value) if isinstance(value, NamedTuple) else value for value in o)
    return dict(zip(o._fields, values))


def test_serial():
    # m = BaseMessage.now('log.info')
    m = LogMessage.now('INFO', 'Running')
    assert to_json(named_tuple_to_dict(m)) == {''}


class JsonLinesWriter:
    def __init__(self, file_obj) -> None:
        self._file_obj = file_obj

    def __enter__(self):
        return self

    def write_item(self, item):
        self._file_obj.write(to_json(item) + '\n')
        self._file_obj.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_obj.close()


def to_json(o, *args, **kwargs):
    """
    Support a few more common types for json serialisation

    Let's make the output slightly more useful for common types.

    >>> to_json([1, 2])
    '[1, 2]'
    >>> # Sets and paths
    >>> to_json({pathlib.Path('/tmp')})
    '["/tmp"]'
    >>> to_json(uuid.UUID('b6bf8ff5-99e6-4562-87b4-cbe6549335e9'))
    '"b6bf8ff5-99e6-4562-87b4-cbe6549335e9"'
    """
    return json.dumps(
        o,
        default=_json_fallback,
        separators=(', ', ':'),
        sort_keys=True,
    )


def _json_fallback(obj):
    """Fallback for non-serialisable json types."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, (pathlib.Path, uuid.UUID)):
        return str(obj)

    if isinstance(obj, set):
        return list(obj)

    try:
        # Allow class to define their own.
        return obj.to_dict()
    except AttributeError:
        # Same behaviour to structlog default: we always want to log the event
        return repr(obj)
