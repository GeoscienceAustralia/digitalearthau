#!/usr/bin/env bash

echoerr() { echo "$@" 1>&2; }

if [[ $# != 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <version>"
    exit 1
fi
export version="$1"

export agdc_env_module=agdc-py3-env/20161201

pyvariant=py3 variant=dev dbhost=130.56.244.225 ./package-instance-module.sh $version
pyvariant=py3 variant=futureprod dbhost=130.56.244.105 ./package-instance-module.sh $version
pyvariant=py3 variant=prod dbhost=130.56.244.227 ./package-instance-module.sh $version

export agdc_env_module=agdc-py2-env/20161201
pyvariant=py2 variant=prod dbhost=130.56.244.227 ./package-instance-module.sh $version

