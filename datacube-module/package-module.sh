#!/usr/bin/env bash

set -eu

umask 002

rm -rf agdc-v2 > /dev/null 2>&1
git clone -b develop https://github.com/data-cube/agdc-v2.git

repository_location="agdc-v2"

export py2_env_version=20160223

module load agdc-py2-env/${py2_env_version}
python --version

pushd ${repository_location}

# We export vars for envsubst below.
export module_dir=/g/data/v10/public/modules
export module_path=${module_dir}/modulefiles
module_requires=`python setup.py --requires`
export version=`python setup.py --version`
export package_name='agdc-py2-demo'
export package_description=`python setup.py --description`

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

    export PYTHONPATH="${PYTHONPATH}:${python_dest}"
    echo "Installing:"
    python setup.py install "--prefix=${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    popd

    cp -v datacube-demo.conf ${package_dest}/datacube.conf

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

rm -rf agdc-v2 > /dev/null 2>&1

echo
echo 'Done.'

