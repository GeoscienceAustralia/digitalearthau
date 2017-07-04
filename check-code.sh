#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

shopt -s globstar

pylint -j 2 --reports no digitalearthau scripts/**/*.py
# 'continuation line' has too many spurious errors.
pep8 --ignore=E122 --max-line-length 120  digitalearthau scripts/**/*.py

# Users can specify extra folders as arguments.
py.test -r sx --durations=5 digitalearthau scripts/**/*.py "$@"

