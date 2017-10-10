import getpass
import json
import os
import socket
import uuid
import datetime

from typing import NamedTuple, Optional, List, Dict, Union

from enum import Enum, unique

import pathlib
import dateutil.parser

import yaml

from digitalearthau import paths


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
        default=simplify_obj,
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


def dump_structure(path: pathlib.Path, object):
    """
    Dump NamedTuples to a yaml/json document
    """
    return dump_document(path, named_tuple_to_dict(object))


def load_structure(path: pathlib.Path, expected_type):
    """
    Load the expected NamedTuple (with type hints) from a yaml/json
    """
    return deserial(paths.read_document(path), expected_type)


def simplify_obj(obj):
    """Make common types serialisable as json/yaml/whatever."""
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
        return obj


def named_tuple_to_dict(o):
    """
    Convert a named tuple and all of its directly-embedded named tuples to serialisable dicts
    """
    # TODO: Doesn't handle indirectly embedded NamedTuples, such as within a list.

    # We can't isinstance() for NamedTuple, the noraml way is to see if attributes like _fields exist.
    try:
        return dict(zip(o._fields, (named_tuple_to_dict(value) for value in o)))
    except AttributeError:
        return simplify_obj(o)


def deserial(o, expected_type):
    """
    Try to parse the given dict into an object tree.
    """
    if expected_type in (pathlib.Path, uuid.UUID):
        return expected_type(o)

    if expected_type == datetime.datetime:
        return dateutil.parser.parse(o)

    # We can't isinstance() for NamedTuple, the noraml way is to see if attributes like _fields exist.
    try:
        field_types: Dict = expected_type._field_types
        assert isinstance(o, dict)
        return expected_type(
            **{k: deserial(v, field_types[k]) for (k, v) in o.items()}
        )
    except AttributeError:
        pass
    return o


def test_serial():
    # Convert to dict and back again, checking that it's identical.

    class EmbeddedObj(NamedTuple):
        a: str
        my_dt: datetime.datetime

    class ObjA(NamedTuple):
        v1: str
        v2: List[int]
        mynamedtuple: EmbeddedObj
        my_path: pathlib.Path

    m = ObjA(
        "a string",
        [1, 2, 3],
        EmbeddedObj(
            "b string",
            datetime.datetime.utcnow()
        ),
        pathlib.Path("/tmp/test")
    )

    d = named_tuple_to_dict(m)
    print(repr(d))
    new_m = deserial(d, ObjA)
    print(repr(new_m))
    assert m == new_m
