#!/usr/bin/env bash

set -eu

# This script's directory
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echoerr() { echo "$@" 1>&2; }

if [[ $# != 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <datacube-version>"
    echoerr
    echoerr "eg. $0 1.1.17"
    exit 1
fi

agdc_version="$1"
py_module_version=$(date +'%Y%m%d')
module_dir="/g/data/v10/public/modules"

pushd "${dir}/py-environment"
    if [ ! -e "${module_dir}/modulefiles/agdc-py3-env/${py_module_version}" ];
    then
        echo
        echo "Creating PY3 Environment"
        ./package-module.sh --variant py3 --moduledir ${module_dir};
    fi

    if [ ! -e "${module_dir}/modulefiles/agdc-py2-env/${py_module_version}" ];
    then
        echo
        echo "Creating PY2 Environment"
        ./package-module.sh --variant py2 --moduledir ${module_dir};
    fi
popd

pushd "${dir}/agdc"
    if [ ! -e "${module_dir}/modulefiles/agdc-py3/${agdc_version}" ];
    then
        echo
        echo "Creating PY3 AGDC ${agdc_version}"
        ./package-module.sh \
            --env agdc-py3-env/${py_module_version} \
            --moduledir ${module_dir} \
            --version ${agdc_version}
    fi
    if [ ! -e "${module_dir}/modulefiles/agdc-py2/${agdc_version}" ];
    then
        echo
        echo "Creating PY2 AGDC ${agdc_version}"
        ./package-module.sh \
            --env agdc-py2-env/${py_module_version} \
            --moduledir ${module_dir} \
            --version ${agdc_version}
    fi
popd


pushd "${dir}/agdc-instances"
    echo
    echo "Creating instances"
    ./package-all-instances.sh ${py_module_version} ${agdc_version}
popd

echo
echo "All done."

