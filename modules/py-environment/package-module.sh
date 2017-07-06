#!/usr/bin/env bash

set -eu

umask 022

variant=py2
export module_dir=/g/data/v10/public/modules

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo "Usage: $0 --variant ${variant} --moduledir ${module_dir}"
        exit 0
        ;;
    --variant)
        variant="$2"
        shift
        ;;
    --conda)
        conda_url="$2"
        shift # past argument
        ;;
    --moduledir)
        export module_dir="$2"
        shift # past argument
        ;;
    *)
        echo Unknown option argument "$1"
        exit 1
        ;;
    esac
shift # past argument or value
done

case $variant in
py2)
    python='2.7'
    conda_url=https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
    ;;
py3)
    python='3.6'
    conda_url=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    ;;
*)
    echo Unknown variant. Must be one of py2, py3
    exit 1
    ;;
esac


tmp_dir="$(mktemp -d)"

package_name=agdc-${variant}-env
module_path=${module_dir}/modulefiles
version=$(date +'%Y%m%d')
package_description="Datacube environment module"
package_dest="${module_dir}/${package_name}/${version}"

# We export vars for envsubst below.
export package_name module_path version package_description package_dest

echo "# Packaging '$package_name' '$version' to '$package_dest' #"

read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    wget ${conda_url} -O "${tmp_dir}/miniconda.sh"
    bash "${tmp_dir}/miniconda.sh" -b -p "${package_dest}"

    # The root folder (but not its contents) is missing public read/execute by default (?)
    chmod a+rx "${package_dest}"

    "${package_dest}/bin/conda" config --prepend channels conda-forge --system
    # update root env to the latest python and packages
    "${package_dest}/bin/conda" update --all -y

    # append required version of python
    cp environment.yaml "${tmp_dir}/env.yaml"
    echo "- python=${python}" >> "${tmp_dir}/env.yaml"

    # make sure no .local stuff interferes with the install
    export PYTHONNOUSERSITE=1

    # create the env
    "${package_dest}/bin/conda" env create --file "${tmp_dir}/env.yaml"

    chmod -R a-w "${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    modulefile_dest="${modulefile_dir}/${version}"
    esc='$' envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

echo
echo 'Done.'

