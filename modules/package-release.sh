#!/usr/bin/env bash

set -eu

# This script's directory
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echoerr() { echo "$@" 1>&2; }

if [[ $# -lt 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <datacube-version> [py-env-version]"
    echoerr
    echoerr "eg. $0 1.1.17"
    echoerr "or  $0 1.1.17 20170704"
    exit 1
fi

agdc_version="$1"
py_module_version=${2:-$(date +'%Y%m%d')}
module_dir="/g/data/v10/public/modules"


function build_variant() {
    py=$1
    pushd "${dir}/py-environment"
        if [ ! -e "${module_dir}/modulefiles/agdc-${py}-env/${py_module_version}" ];
        then
            echo
            echo "Creating ${py} Environment"
            version="${py_module_version}" ./package-module.sh --variant "${py}" --moduledir "${module_dir}";
        fi
    popd

    pushd "${dir}/agdc"
        if [ ! -e "${module_dir}/modulefiles/agdc-${py}/${agdc_version}" ];
        then
            echo
            echo "Creating ${py} AGDC ${agdc_version}"
            ./package-module.sh \
                --env "agdc-${py}-env/${py_module_version}" \
                --moduledir "${module_dir}" \
                --version "${agdc_version}"
        fi
    popd

    pushd "${dir}/agdc-instances"
        echo
        echo "Creating instances"
        ./package-all-instances.sh "${py_module_version}" "${agdc_version}"
    popd

    echo
    echo "=========================="
    echo "= Variant ${py} completed. ="
    echo "=========================="
    echo
}

build_variant py3
build_variant py2

echo
echo "All done."

