#!/usr/bin/env bash

set -eu

umask 002

variant=dev
export module_dir=/g/data/v10/public/modules
export agdc_env_module=agdc-py2-env/anaconda2-2.5.0

export LC_ALL=en_AU.utf8
export LANG=C.UTF-8

while [[ $# > 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo Usage: $0 --env ${agdc_env_module} --moduledir ${module_dir} --version 1.1.9
        echo "  Options:"
        echo "    --env agdc_env_module     Python module to use."
        echo "    --moduledir module_dir    Destination module path."
        echo "    --version version         GitHub tag to use. ie. 'datacube-<version>'. Default is '<HEAD>'"
	echo
        exit 0
        ;;
    --version)
        export version="$2"
        shift # past argument
        ;;
    --env)
        export agdc_env_module="$2"
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

module load ${agdc_env_module}
python_version=`python -c 'from __future__ import print_function; import sys; print("%s.%s"%sys.version_info[:2])'`
python_major=`python -c 'from __future__ import print_function; import sys; print(sys.version_info[0])'`
subvariant=py${python_major}

rm -rf agdc-v2 > /dev/null 2>&1
git clone -b develop https://github.com/data-cube/agdc-v2.git
pushd agdc-v2
if [ ! -z ${version+x} ]
then
  git checkout tags/datacube-${version} -b module_package
fi

# run tests TODO: integration tests?
python setup.py test

export version=`python setup.py --version`

export package_name=agdc-${subvariant}
export package_description=`python setup.py --description`

export package_dest=${module_dir}/${package_name}/${version}
export python_dest=${package_dest}/lib/python${python_version}/site-packages

echo '# Packaging '$package_name' v '$version' to '$package_dest' #'

read -p "Continue? [y/N]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then

    # Setuptools requires the destination to be on the path, as it tests that the target is loadable.
    echo "Creating directory"
    mkdir -v -p "${python_dest}"

    echo "Installing:"
    python setup.py sdist
    pip install dist/datacube-${version}.tar.gz --no-deps --prefix "${package_dest}"
    #python setup.py install "--prefix=${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    popd

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

rm -rf agdc-v2 > /dev/null 2>&1


read -p "Do you want to also install datacube-stats? [y/N]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    export PYTHONPATH=${PYTHONPATH:+${PYTHONPATH}:}${python_dest}
    # Install datacube-stats as well
    rm -rf agdc_statistics > /dev/null 2>&1
    git clone https://github.com/GeoscienceAustralia/agdc_statistics.git
    pushd agdc_statistics
    python setup.py sdist
    pip install dist/*.tar.gz --no-deps --prefix "${package_dest}"
    popd
fi

echo
echo 'Done.'

