#!/usr/bin/env bash

set -eu

umask 002

repository_location=$1

export py2_env_version=20160128

module load datacube-py2-env/${py2_env_version}

pushd ${repository_location}

# We export vars for envsubst below.
export module_dir=/projects/u46/opt/modules
#export module_dir=/short/v10/jmh547/modules
export module_path=${module_dir}/modulefiles
export version=`./setup.py --version`
export package_name='datacube-demo-env'
export package_description=`./setup.py --description`

export package_dest="${module_dir}/${package_name}/${version}"
export python_dest="${package_dest}/lib/python2.7/site-packages"

echo "# Packaging '$package_name' v '$version' to '$package_dest' #"

read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Setuptools requires the destination to be on the path, as it tests that the target is loadable.
    echo "Creating directory"
    mkdir -v -p "${python_dest}"

    PYTHONPATH="$PYTHONPATH:${python_dest}"
    echo "Installing:"
    ./setup.py install "--prefix=${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    popd

    cp -v datacube-demo.conf ${package_dest}/datacube.conf

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

echo
echo 'Done.'

