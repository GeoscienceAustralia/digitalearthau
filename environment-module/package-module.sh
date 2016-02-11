#!/usr/bin/env bash

set -eu

umask 002

export python_version='2.7.5'
# TODO: Datacube tests fail when using gdal 2.0.0
export gdal_version='1.11.1-python'
export netcdf_version='4.3.3.1'
export hdf5_version='1.8.14'

module load setuptools

# Our install Python & gdal must match the ones run.
module load python/${python_version}
module load netcdf/${netcdf_version}
module load gdal/${gdal_version}
module load hdf5/${hdf5_version}

all_deps="$*"
echo $all_deps

export package_name=agdc-py2-env

# We export vars for envsubst below.
export module_dir=/g/data/v10/public/modules
export module_path=${module_dir}/modulefiles
export version=$(date +'%Y%m%d')

export package_dest="${module_dir}/${package_name}/${version}"
export python_dest="${package_dest}/lib/python2.7/site-packages"


# Compile deps with gcc, to match Python. (otherwise we get undefined intel symbols).
export CC='gcc'

echo "# Packaging '$package_name' '$version' to '$package_dest' #"

read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Setuptools requires the destination to be on the path, as it tests that the target is loadable.
    echo "Creating directory"
    mkdir -v -p "${python_dest}"

    PYTHONPATH="$PYTHONPATH:${python_dest}"
    # The linux 64bit Dask tarball that pip austomatically installs is broken.
    echo "Installing:"
    easy_install "--prefix=${package_dest}" ${all_deps} dask-0.7.5.tar.gz matplotlib-1.5.1.tar.gz

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

echo
echo 'Done.'

