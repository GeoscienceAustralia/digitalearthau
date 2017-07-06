#!/usr/bin/env bash

set -eu

echoerr() { echo "$@" 1>&2; }

if [[ $# != 2 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <py-module-version> <datacube-version>"
    echoerr
    echoerr "eg. $0 20161201 1.1.17"
    exit 1
fi
export py_module_version="$1"
export version="$2"

export agdc_env_module=agdc-py3-env/${py_module_version}

pyvariant=py3 variant=dev dbhost=130.56.244.225 ./package-instance-module.sh "$version"
pyvariant=py3 variant=oldprod dbhost=130.56.244.227 ./package-instance-module.sh "$version"
pyvariant=py3 variant=prod dbhost=130.56.244.105 ./package-instance-module.sh "$version"


export agdc_env_module=agdc-py2-env/${py_module_version}

pyvariant=py2 variant=prod dbhost=130.56.244.105 ./package-instance-module.sh "$version"

