import enum
import json
import pathlib
import uuid

import datetime
import dateutil.parser
import yaml
from typing import Dict

from digitalearthau import paths
from digitalearthau.events import Status


class JsonLinesWriter:
    def __init__(self, file_obj) -> None:
        self._file_obj = file_obj

    def __enter__(self):
        return self

    def write_item(self, item):
        self._file_obj.write(to_lenient_json(type_to_dict(item), compact=True) + '\n')
        self._file_obj.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_obj.close()


def to_lenient_json(o, compact=False, *args, **kwargs):
    """
    Convert to json, supporting a few more common data types (Paths, UUID).

    Will always return a value (falling back to repr() rather than throw an unsupported object exception). This
    is intended for cases such as logs where you always want output (need reliability).

    Readable, indented, by default. Use compact=True for single-line output like jsonl

    >>> to_lenient_json([1, 2], compact=True)
    '[1, 2]'
    >>> to_lenient_json({'a': 1})
    '{\\n    "a": 1\\n}'
    >>> # Sets and paths
    >>> to_lenient_json({pathlib.Path('/tmp')}, compact=True)
    '["/tmp"]'
    >>> to_lenient_json(uuid.UUID('b6bf8ff5-99e6-4562-87b4-cbe6549335e9'))
    '"b6bf8ff5-99e6-4562-87b4-cbe6549335e9"'
    """
    return json.dumps(
        o,
        default=_lenient_json_fallback,
        separators=(', ', ':') if compact else None,
        sort_keys=True,
        indent=None if compact else 4
    )


def _lenient_json_fallback(obj):
    """Fallback that should always succeed.

    The default fallback will throw exceptions for unsupported types, this one will always
    at least repr() an object rather than throw a NotSerialisableException

    (intended for use in places such as json-based logs where you always want the message recorded)
    """
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


def dump_document(path: pathlib.Path, obj, allow_unsafe=False):
    """
    Write the given object to a file (json/yaml), in readable/indented format

    The format is chosen based on the file suffix.
    """
    suffix = path.suffix.lower()
    if suffix == '.yaml':
        path.write_text(
            # Allow unsafe dump, for where it's used as a more readable pickle.
            yaml.dump(
                obj,
                Dumper=yaml.Dumper if allow_unsafe else yaml.SafeDumper,
                default_flow_style=False,
                indent=4,
            )
        )
    elif suffix == '.json':
        path.write_text(
            to_lenient_json(obj) + '\n'
        )
    else:
        raise NotImplementedError(f"Unknown suffix {suffix}. Expected json/yaml.")


def dump_structure(path: pathlib.Path, obj):
    """
    Dump NamedTuples and other simple objects to a yaml/json document
    """
    return dump_document(path, type_to_dict(obj))


def load_structure(path: pathlib.Path, expected_type):
    """
    Load the expected NamedTuple (with type hints) from a yaml/json

    :param expected_type: the class/type you expect to get back.
    """
    return dict_to_type(paths.read_document(path), expected_type)


def simplify_obj(obj):
    """Make common types serialisable as json/yaml/whatever."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    if isinstance(obj, (pathlib.Path, uuid.UUID)):
        return str(obj)

    if isinstance(obj, set):
        return list(obj)

    if isinstance(obj, enum.Enum):
        # Our enums are serialised for json to lowercase representation, as with most of our identifiers
        return obj.name.lower()

    try:
        # Allow class to define their own.
        return obj.to_dict()
    except AttributeError:
        return obj


def type_to_dict(o):
    """
    Convert a named tuple and all of its directly-embedded named tuples to dicts/etc suitable for json/yaml


    TODO: Doesn't handle indirectly embedded NamedTuples, such as within a list.
    (It's currently only used for simple cases, not complex hierarchies)

    >>> type_to_dict(Status.ACTIVE)
    'active'
    >>> # More tests in test_serialise.py
    """

    # We can't isinstance() for NamedTuple, the noraml way is to see if attributes like _fields exist.
    try:
        return dict(zip(o._fields, (type_to_dict(value) for value in o)))
    except AttributeError:
        return simplify_obj(o)


def dict_to_type(o, expected_type):
    """
    Try to parse the given dict (from json/etc) into the given NamedTuple.

    TODO: Doesn't handle indirectly embedded NamedTuples, such as within a list, or Optionals/Unions/etc
    (It's currently only used for simple cases, not complex hierarchies)

    >>>
    >>> Status.ACTIVE == dict_to_type('active', expected_type=Status)
    True
    >>> dict_to_type('eating', expected_type=Status)
    Traceback (most recent call last):
    ...
    digitalearthau.serialise.SerialisationError: Unknown field EATING for Status. Expected one of ...
    >>> # More tests in test_serialise.py
    """
    if o is None:
        return None

    if expected_type in (pathlib.Path, uuid.UUID):
        return expected_type(o)

    if expected_type == datetime.datetime:
        return dateutil.parser.parse(o)

    if issubclass(expected_type, enum.Enum):
        name = str(o).upper()
        v = expected_type.__members__.get(name)
        if not v:
            raise SerialisationError(f"Unknown field {name} for {expected_type.__name__}. "
                                     f"Expected one of {', '.join(expected_type.__members__)}")
        return v
    try:
        # We can't isinstance() for NamedTuple, the noraml way is to see if attributes like _fields exist.
        # pylint: disable=protected-access
        field_types: Dict = expected_type._field_types
        assert isinstance(o, dict)
        return expected_type(
            **{k: dict_to_type(v, field_types[k]) for (k, v) in o.items()}
        )
    except AttributeError:
        pass
    return o


class SerialisationError(Exception):
    pass
