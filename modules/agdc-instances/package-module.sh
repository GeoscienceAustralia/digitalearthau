#!/usr/bin/env bash

set -eu

umask 002

export module_dir=/g/data/v10/public/modules
export agdc_env_module=agdc-py2-env/anaconda2-2.5.0
export pyvariant=py2
export variant=prod
export dbhost=130.56.244.227
export dbport=6432
export dbname=datacube

while [[ $# > 0 ]]
do
    key="$1"

    case $key in
    --help)
        echo Usage: $0 --moduledir ${module_dir} --variant ${pyvariant} --name ${variant} --version "<version>" --dbname $dbname --dbhost $dbhost --dbport $dbport
        exit 0
        ;;
    --moduledir)
        export module_dir="$2"
        shift # past argument
        ;;
   --name)
        export variant="$2"
        shift # past argument
        ;;
    --version)
        export version="$2"
        shift # past argument
        ;;
    --variant)
        export pyvariant="$2"
        shift # past argument
        ;;
    *)
        echo Unknown option argument "$1"
        exit 1
        ;;
    esac
shift # past argument or value
done

export agdc_module=agdc-${pyvariant}
export module_name=agdc-${pyvariant}-${variant}
export module_dest=${module_dir}/${module_name}/${version}
export module_description="AGDC database configuration"


echo "###############"
echo "agdc_env_module = ${agdc_env_module}"
echo "pyvariant = ${pyvariant}"
echo "variant = ${variant}"
echo "dbhost = ${dbhost}"
echo "dbport = ${dbport}"
echo "dbname = ${dbname}"
echo "###############"
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
cp -v datacube-ensure-user.sh "${module_dest}/"
chmod 775 "${module_dest}/datacube-ensure-user.sh"

echo "[datacube]" > "${module_dest}/datacube.conf"
echo db_hostname: ${dbhost} >> "${module_dest}/datacube.conf"
echo db_port: ${dbport} >> "${module_dest}/datacube.conf"
echo db_database: ${dbname} >> "${module_dest}/datacube.conf"

modulefile_dir="${module_dir}/modulefiles/${module_name}"
mkdir -v -p "${modulefile_dir}"

render modulefile.template "${modulefile_dir}/${version}"

