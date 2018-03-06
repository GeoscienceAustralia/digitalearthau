#!/usr/bin/env bash

set -eu

umask 002
unset PYTHONPATH

echo "##########################"
echo
echo "module_dir = ${module_dir:=/g/data/v10/private/modules}"
echo "dea_module_dir = ${dea_module_dir:=/g/data/v10/public/modules}"
echo
echo "dea_module = ${dea_module:=agdc-py3-prod/1.3.2}"
dea_module_name=${dea_module%/*}
instance=${dea_module_name##*-}
echo "instance = ${instance}"
echo
echo "eodatasets_head = ${eodatasets_head:=develop}"
echo "gqa_head = ${gqa_head:=develop}"
echo "gaip_head = ${gaip_head:=develop}"
echo
echo "##########################"
export module_dir dea_module

echoerr() { echo "$@" 1>&2; }

if [[ $# != 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr
    echoerr "Usage: $0 <version>"
    exit 1
fi
export version="$1"

module use ${module_dir}/modulefiles
module use -a ${dea_module_dir}/modulefiles
module load ${dea_module}

python_version=$(python -c 'from __future__ import print_function; import sys; print("%s.%s"%sys.version_info[:2])')
python_major=$(python -c 'from __future__ import print_function; import sys; print(sys.version_info[0])')
subvariant=py${python_major}


function installrepo() {
    destination_name=$1
    head=${2:=develop}
    repo=$3

    repo_cache="cache/${destination_name}.git"

    if [ -e "${repo_cache}" ]
    then
        pushd "${repo_cache}"
            git remote update
        popd
    else
        git clone --mirror "${repo}" "${repo_cache}"
    fi

    build_dest="build/${destination_name}"
    [ -e "${build_dest}" ] && rm -rf "${build_dest}"
    git clone -b "${head}" "${repo_cache}" "${build_dest}"

    pushd "${build_dest}"
        rm -r dist build > /dev/null 2>&1 || true
        python setup.py sdist
        pip install dist/*.tar.gz "--prefix=${package_dest}"
    popd
}

package_name=galpgs-${subvariant}-${instance}
package_description="GA lpgs processing"
package_dest=${module_dir}/${package_name}/${version}
python_dest=${package_dest}/lib/python${python_version}/site-packages
export package_name package_description package_dest python_dest

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
    installrepo idlfunctions develop              git@github.com:sixy6e/idl-functions.git
    installrepo eotools      develop              git@github.com:GeoscienceAustralia/eo-tools.git
    installrepo eodatasets   "${eodatasets_head}" git@github.com:GeoscienceAustralia/eo-datasets.git
    installrepo gaip         "${gaip_head}"       git@github.com:GeoscienceAustralia/gaip.git
    installrepo gqa          "${gqa_head}"        git@github.com:GeoscienceAustralia/gqa.git

    echo
    echo "Installing galpgs"
    installrepo galpgs      "${version}"          git@github.com:jeremyh/galpgs.git

    echo
    echo "Writing modulefile"
    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"
    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

rm -rf build > /dev/null 2>&1


echo
echo 'Done.'

