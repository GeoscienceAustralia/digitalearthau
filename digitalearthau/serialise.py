"""
Tools for conversion to/from json, jsonl, yaml.

This especially revolves around conversion of NamedTuples, as they are used in many of our
common types (events, runner model objects)

(The use of named tuples were an attempt to be a compromise between dicts and full classes.
A lot of the previous dict-based code grew unwieldy, especially as it neutered pycharm/pylint.
Normal classes are an option too, but they'd need their own serialization code regardless.)
"""
import datetime
import enum
import json
import pathlib
import uuid

import cattr
import dateutil.parser
import yaml
from cattr import unstructure

from digitalearthau import paths
from digitalearthau.events import Status


class JsonLinesWriter:
    """
    Stream events (or any Namedtuple) to a file in JSON-Lines format.
    """

    def __init__(self, file_obj) -> None:
        self._file_obj = file_obj

    def __enter__(self):
        return self

    def write_item(self, item):
        self._file_obj.write(to_lenient_json(type_to_dict(item), compact=True) + '\n')
        self._file_obj.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file_obj.close()


# The "args" and "kwargs" are here to allow use as a json.dumps() replacement (ignoring other arguments).
# pylint: disable=keyword-arg-before-vararg
def to_lenient_json(o, compact=False, *args, **kwargs):
    """
    Convert an object to json, supporting a few more common data types (Paths, UUID).

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

    >>> type_to_dict(Status.ACTIVE)
    'active'
    >>> # More tests in test_serialise.py
    """
    # This was greatly simplified now that cattrs exists.
    return unstructure(o)


def _structure_as_uuid(d, t) -> uuid.UUID:
    return uuid.UUID(str(d))


def _structure_as_datetime(d, t) -> datetime.datetime:
    if isinstance(d, datetime.datetime):
        return d
    return dateutil.parser.parse(d)


def _structure_as_pathlib(d, t) -> pathlib.Path:
    if isinstance(d, pathlib.Path):
        return d
    return pathlib.Path(d)


def _passthrough(d, t) -> dict:
    return d


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
    ValueError: 'eating' is not a valid Status
    >>> # More tests in test_serialise.py
    """
    if o is None:
        return None

    c = cattr.Converter()
    c.register_structure_hook(uuid.UUID, _structure_as_uuid)
    c.register_structure_hook(datetime.datetime, _structure_as_datetime)
    c.register_structure_hook(pathlib.Path, _structure_as_pathlib)

    # Needed for cattrs 1.0.0: our properties are a raw dict, so do nothing to them.
    c.register_structure_hook(dict, _passthrough)
    return c.structure(o, expected_type)


class MultilineString(str):
    """
    A string that will be represented preserved in multi-line format in yaml.
    """

    pass


def literal_presenter(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')


yaml.add_representer(MultilineString, literal_presenter)


def as_string_representer(dumper, data):
    """
    :type dumper: yaml.representer.BaseRepresenter
    :type data: uuid.UUID
    :rtype: yaml.nodes.Node
    """
    return dumper.represent_scalar(u'tag:yaml.org,2002:str', '%s' % data)


yaml.add_multi_representer(pathlib.Path, as_string_representer)
