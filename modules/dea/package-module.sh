#!/usr/bin/env bash

echoerr() { echo "$@" 1>&2; }

if [ -n "${PYTHONPATH}" ];
then
    echoerr "ERROR: PYTHONPATH is already set. Please build from a clean shell without modules loaded."
    exit 1
fi

set -eu
umask 002

# Default module dirs. You can set the variables before calling this script to override them.
agdc_module_dir="${agdc_module_dir:-/g/data/v10/public/modules}"
module_dir="${module_dir:-/g/data/v10/public/modules}"


if [[ $# -lt 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <dea_head> [<datacube_module]"
    echoerr
    echoerr "Examples:"
    echoerr "   $0 develop"
    echoerr "   $0 2017.1 agdc-py3-prod/1.5.1"
    exit 1
fi
export version="$1"
agdc_instance_module="${2:-agdc-py3-prod}"


if [ ! -e "${agdc_module_dir}/modulefiles/${agdc_instance_module}" ];
then
    echoerr "No module found for '${agdc_instance_module}' in '${agdc_module_dir}'"
    echoerr
    echoerr "Expected something like 'agdc-py3-prod/1.5.1'"
    exit 1
fi

agdc_instance_module_name=${agdc_instance_module%/*}
instance=${agdc_instance_module_name##*-}

echo "####################################################"
echo "- DEA"
echo "-      instance: ${instance}"
echo "-       version: ${version}"
echo "-           dir: ${module_dir}"
echo "----------------------------------------------------"
echo "- Dependencies"
echo "-"
echo "- agdc"
echo "-        module: ${agdc_instance_module}"
echo "-           dir: ${agdc_module_dir}"
echo "- eo-datasets"
echo "-          head: ${eodatasets_head:=develop}"
echo "####################################################"
export module_dir agdc_instance_module


module use "${module_dir}/modulefiles"
module use -a "${agdc_module_dir}/modulefiles"
module load "${agdc_instance_module}"

python_version=$(python -c 'from __future__ import print_function; import sys; print("%s.%s"%sys.version_info[:2])')


export package_name=dea-${instance}
export package_description="DEA tools for NCI"

export package_dest=${module_dir}/${package_name}/${version}
export python_dest=${package_dest}/lib/python${python_version}/site-packages

printf '# Packaging "%s %s" to "%s" #\n' "$package_name" "$version" "$package_dest"

read -p "Continue? [y/N]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Creating directory"
    mkdir -v -p "${python_dest}"
    # The destination needs to be on the path so that latter dependencies can see earlier ones
    export PYTHONPATH=${PYTHONPATH:+${PYTHONPATH}:}${python_dest}

    echo
    echo "Installing dependencies"
    installrepo eodatasets "${eodatasets_head}" git@github.com:GeoscienceAustralia/eo-datasets.git

    echo
    echo "Installing digitalearthau"
    installrepo dea "${version}" git@github.com:GeoscienceAustralia/digitalearthau.git

    mkdir -v -p "${module_dest}"
    cp -v datacube-ensure-user.py "${module_dest}/"
    chmod 775 "${module_dest}/datacube-ensure-user.py"

    # Releases should be immutable once built (other than develop)
    [[ "${version}" == 'develop' ]] || chmod -R a-w "${package_dest}"

    echo
    echo "Writing modulefile"
    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"
    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"

    # Modulefile should be immutable once built (other than develop)
    [[ "${version}" == 'develop' ]] || chmod -R a-w "${modulefile_dest}"
fi

rm -rf build > /dev/null 2>&1


echo
echo 'Done.'

