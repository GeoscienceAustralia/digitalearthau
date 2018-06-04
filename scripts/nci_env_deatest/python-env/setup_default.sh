#!/usr/bin/env bash
# source this file to set up dea-env module and local packages

LANG=en_AU.UTF-8

export SANTOSH=/g/data/u46/users/sm9911
export ENV="$SANTOSH/python-env"

if [ "$#" -eq 1 ]
then
   DC_CONFIG_PATH="$SANTOSH/$1"
else
   DC_CONFIG_PATH="$ENV/datacube.conf"
fi

module use /g/data/v10/public/modules/modulefiles
if [[ -n "$(module avail git 2>&1)" ]]; then
    module load git
fi

module load dea-env
module load udunits

[[ -z "${DATACUBE_CONFIG_PATH}" ]] && export DATACUBE_CONFIG_PATH=$DC_CONFIG_PATH
[[ ":$PATH:" != *":$ENV/bin:"* ]] && export PATH="$ENV/bin:$PATH"
export PYTHONUSERBASE="$ENV"
unset PYTHONNOUSERSITE
