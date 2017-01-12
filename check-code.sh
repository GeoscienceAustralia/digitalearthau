#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

py_files=$(find scripts -name '*.py')

pylint -j 2 --reports no ${py_files}

# Users can specify extra folders as arguments.
py.test -r sx --durations=5 ${py_files} $@

