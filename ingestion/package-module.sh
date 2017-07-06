#!/usr/bin/env bash

set -eu

umask 002

export module_dir=/g/data/v10/public/modules
export agdc_module=agdc-py3-prod
export module_description="Datacube ingester utilities and configuration"

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo "Usage: $0 --moduledir ${module_dir} --agdc ${agdc_module}"
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

module_name=agdc-ingester
version=$(git describe --tags --always)
module_dest=${module_dir}/${module_name}/${version}
export module_name version module_dest

printf '# Packaging "%s %s" to "%s" #\n' "$module_name" "$version" "$module_dest"

read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line

function render {
    # shellcheck disable=SC2016
    vars='$module_dir:$agdc_module:$module_description:$module_name:$version:$module_dest'
    envsubst "$vars" < "$1" > "$2"
    echo Wrote "$2"
}

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 0
fi

mkdir -v -p "${module_dest}/scripts"
cp -v -r scripts "${module_dest}/"
cp -v -r products "${module_dest}/"

render scripts/distributed.sh "${module_dest}/scripts/distributed.sh"

mkdir -v "${module_dest}/config"
for i in config/*.yaml
do
  render "${i}" "${module_dest}/${i}"
done

echo module use "${module_dir}/modulefiles" > "${module_dest}/scripts/environment.sh"
echo module load "${agdc_module}" >> "${module_dest}/scripts/environment.sh"

modulefile_dir="${module_dir}/modulefiles/${module_name}"
mkdir -v -p "${modulefile_dir}"

render modulefile.template "${modulefile_dir}/${version}"

