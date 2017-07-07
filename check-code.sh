#!/usr/bin/env bash
# Convenience script for running Travis-like checks.

set -eu
set -x

shopt -s globstar

pylint -j 2 --reports no \
    digitalearthau \
    scripts/**/*.py

# We check the integration tests even though they aren't run by default here.
pylint --rcfile=integration_tests/pylintrc integration_tests/**/*.py

# 'continuation line' has too many spurious errors.
pep8 --ignore=E122 --max-line-length 120  \
    digitalearthau \
    integration_tests \
    scripts/**/*.py

shellcheck ./**/*.sh 

# Users can specify extra folders (ie. integration_tests) as arguments.
py.test -r sx --durations=5 digitalearthau scripts/**/*.py "$@"

