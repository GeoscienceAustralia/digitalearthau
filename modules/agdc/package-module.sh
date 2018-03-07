#!/usr/bin/env bash

set -eu

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

umask 002

export module_dir=/g/data/v10/public/modules

export LC_ALL=en_AU.utf8
export LANG=C.UTF-8

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo "Usage: $0 --env ${agdc_env_module} --moduledir ${module_dir} --version 1.1.9"
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

module use "${module_dir}/modulefiles"
module load "${agdc_env_module}"
python_version=$(python -c 'from __future__ import print_function; import sys; print("%s.%s"%sys.version_info[:2])')
python_major=$(python -c 'from __future__ import print_function; import sys; print(sys.version_info[0])')
subvariant="py${python_major}"


temp_dir="$(mktemp -d)"
cd "${temp_dir}"
rm -rf datacube-core > /dev/null 2>&1
git clone -b develop https://github.com/opendatacube/datacube-core.git
pushd datacube-core
if [ ! -z "${version+x}" ]
then
  git checkout "tags/datacube-${version}" -b module_package
fi

# run tests TODO: integration tests?
python setup.py test

version="$(python setup.py --version)"

package_name=agdc-${subvariant}
package_description="$(python setup.py --description)"

package_dest=${module_dir}/${package_name}/${version}
python_dest=${package_dest}/lib/python${python_version}/site-packages

export version package_name package_description package_dest python_dest

printf '# Packaging "%s %s" to "%s" #\n' "$package_name" "$version" "$package_dest"

read -p "Continue? [y/N]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then

    # Setuptools requires the destination to be on the path, as it tests that the target is loadable.
    echo "Creating directory"
    mkdir -v -p "${python_dest}"

    echo "Installing:"
    python setup.py sdist
    pip install "dist/datacube-${version}.tar.gz" --no-deps --prefix "${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    modulefile_dest="${modulefile_dir}/${version}"
    envsubst < "${script_dir}/modulefile.template" > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi
popd


# Should be immutable once built.
chmod -R a-w "${package_dest}"

echo
echo 'Done.'

