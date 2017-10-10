import getpass
import json
import os
import socket
import uuid
import datetime
from typing import NamedTuple, Optional, List

from enum import Enum, unique

import pathlib

import yaml


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


def dump_document(path: pathlib.Path, object, allow_unsafe=False):
    suffix = path.suffix.lower()
    if suffix == '.yaml':
        path.write_text(
            # Allow unsafe dump, for where it's used as a more readable pickle.
            yaml.dump(
                object,
                Dumper=yaml.Dumper if allow_unsafe else yaml.SafeDumper,
                default_flow_style=False,
                indent=4,
            )
        )
    elif suffix == '.json':
        path.write_text(
            to_json(object)
        )
