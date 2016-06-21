#!/usr/bin/env bash

set -eu

umask 002

variant=dev
module_dir=/g/data/v10/public/modules
export agdc_module=agdc-py2-prod
export module_description="Datacube ingester utilities and configuration"

while [[ $# > 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo Usage: $0 --moduledir ${module_dir} --agdc ${agdc_module}
        exit 0
        ;;
    --agdc)
        export agdc_module="$2"
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

export module_name=agdc-ingester
export version=`git describe --always`

export module_dest=${module_dir}/${module_name}/${version}

echo '# Packaging '$module_name' v '$version' to '$module_dest' #'
read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line

function render {
    envsubst < "$1" > "$2"
    echo Wrote "$2"
}

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 0
fi

mkdir -v -p "${module_dest}"
cp -v -r config "${module_dest}/"
cp -v -r scripts "${module_dest}/"

echo module use "${module_dir}/modulefiles" > "${module_dest}/scripts/environment.sh"
echo module load ${agdc_module} >> "${module_dest}/scripts/environment.sh"

modulefile_dir="${module_dir}/modulefiles/${module_name}"
mkdir -v -p "${modulefile_dir}"

render modulefile.template "${modulefile_dir}/${version}"

