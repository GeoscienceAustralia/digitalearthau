#!/usr/bin/env bash

echo "Checking dependencies..."
dependencies=`./get-dependencies.sh`
./package-module.sh setuptools pip pytest pytest-runner pytest-cov hypothesis pylint virtualenv ipython==4.0.0 jupyter ${dependencies}

