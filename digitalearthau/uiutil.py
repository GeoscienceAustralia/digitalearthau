import datetime
import json
import pathlib
import uuid
from pathlib import Path

import structlog
import sys


class CleanConsoleRenderer(structlog.dev.ConsoleRenderer):
    def __init__(self, pad_event=25):
        super().__init__(pad_event)
        # Dim debug messages
        self._level_to_color['debug'] = structlog.dev.DIM


def init_logging():
    # Direct structlog into standard logging.
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer() if sys.stdout.isatty() else structlog.processors.JSONRenderer(serializer=_to_json),
        ],
        context_class=dict,
        cache_logger_on_first_use=True,
        logger_factory=structlog.PrintLoggerFactory(),
    )


def _to_json(o, *args, **kwargs):
    """
    structlog by default writes the repr() of unknown objects.

    Let's make the output slightly more useful for common types.

    >>> _to_json([1, 2])
    '[1, 2]'
    >>> # Sets and paths
    >>> _to_json({Path('/tmp')})
    '["/tmp"]'
    >>> _to_json(uuid.UUID('b6bf8ff5-99e6-4562-87b4-cbe6549335e9'))
    '"b6bf8ff5-99e6-4562-87b4-cbe6549335e9"'
    """
    return json.dumps(
        o,
        default=_json_fallback,
        separators=(', ', ':'),
        sort_keys=True
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
