import sys
from functools import partial

import structlog

from digitalearthau import serialise


class CleanConsoleRenderer(structlog.dev.ConsoleRenderer):
    def __init__(self, pad_event=25):
        super().__init__(pad_event)
        # Dim debug messages
        self._level_to_color['debug'] = structlog.dev.DIM


def init_logging(output_file=None):
    """
    Setup structlog for structured logging output.

    Note:

     - structured logging (here) defaults to stdout
     - "unstructured" text logging (eg. datacube core) defaults to stderr

    This is because the former is treated as the actual outputs of the scripts we run: something you may pipe
    into another program. The unstructured logs are purely informational.
    """

    if output_file is None:
        output_file = sys.stdout

    # Direct structlog into standard logging.
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="ISO"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Coloured output if to terminal.
            CleanConsoleRenderer() if output_file.isatty()
            else structlog.processors.JSONRenderer(serializer=partial(serialise.to_lenient_json, compact=True)),
        ],
        context_class=dict,
        cache_logger_on_first_use=True,
        logger_factory=structlog.PrintLoggerFactory(file=output_file),
    )
