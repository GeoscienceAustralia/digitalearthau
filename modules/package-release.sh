#!/usr/bin/env bash


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


if [ -n "${PYTHONPATH}" ];
then
    echoerr "ERROR: PYTHONPATH is already set. Please build from a clean shell without modules loaded."
    exit 1
fi

set -eu

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

    export agdc_env_module=agdc-${py}-env/${py_module_version}

    pyvariant=${py} variant=dev dbhost=agdcdev-db.nci.org.au ./package-instance-module.sh "${agdc_version}"
    # We can't use the new hostnames because existing users only have the IP in their pgpass file
    pyvariant=${py} variant=prod dbhost=130.56.244.105 ./package-instance-module.sh "${agdc_version}"

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

