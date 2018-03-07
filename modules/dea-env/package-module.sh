#!/usr/bin/env bash

set -eu

umask 022

export module_dir=/g/data/v10/public/modules

python='3.6'
conda_url=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo "Usage: $0 [--conda CONDA_DOWNLOAD_URL] --moduledir ${module_dir}"
        exit 0
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


tmp_dir="$(mktemp -d)"

package_name=dea-env
module_path=${module_dir}/modulefiles
version=${version:-$(date +'%Y%m%d')}
package_description="DEA environment module"
package_dest="${module_dir}/${package_name}/${version}"

# We export vars for envsubst below.
export package_name module_path version package_description package_dest

# TODO, check that we haven't already created an environment module today

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

    # make sure no .local stuff interferes with the install
    export PYTHONNOUSERSITE=1

    # create the env
    "${package_dest}/bin/conda" env create --file "${tmp_dir}/env.yaml"

    # Ensure all file and folders are readable by all
    find "${package_dest}" -type f -print0 | xargs -0 chmod a+r
    find "${package_dest}" -type d -print0 | xargs -0 chmod a+rx

    # Revoke my write access to minimise accidental changes.
    chmod -R a-w "${package_dest}"

    modulefile_dir="${module_dir}/modulefiles/${package_name}"
    mkdir -v -p "${modulefile_dir}"

    # Write the modulefile to enable people to load
    modulefile_dest="${modulefile_dir}/${version}"
    esc='$' envsubst < modulefile.template > "${modulefile_dest}"
    echo "Wrote modulefile to ${modulefile_dest}"
fi

echo
echo 'Done.'

