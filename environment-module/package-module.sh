#!/usr/bin/env bash

set -eu

umask 002

variant=py2
conda_url=http://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
export module_dir=/g/data/v10/public/modules

while [[ $# > 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo Usage: $0 --variant ${variant} --moduledir ${module_dir} --conda ${conda_url}
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
     # unknown option
    ;;
    esac
shift # past argument or value
done


export package_name=agdc-${variant}-env

# We export vars for envsubst below.
export module_path=${module_dir}/modulefiles
export version=$(date +'%Y%m%d')
export package_description="Datacube environment module"
export package_dest="${module_dir}/${package_name}/${version}"

echo "# Packaging '$package_name' '$version' to '$package_dest' #"

read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    wget ${conda_url} -O miniconda.sh
    bash miniconda.sh -b -p ${package_dest}
    ${package_dest}/bin/conda config --add channels conda-forge
    ${package_dest}/bin/conda update --all -y
    ${package_dest}/bin/conda install anaconda-client -y
    ${package_dest}/bin/conda env create --file environment.yaml
    ${package_dest}/bin/conda install -n agdc libgcc -y

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

echo
echo 'Done.'

