#!/usr/bin/env bash

set -eu

umask 002


echo "##########################"
echo "module_dir = ${module_dir:=/g/data/v10/public/modules}"
echo "agdc_env_module = ${agdc_env_module:=agdc-py3-env/20161201}"
echo "pyvariant = ${pyvariant:=py3}"
echo "variant = ${variant:=dev}"
echo "dbhost = ${dbhost:=130.56.244.225}"
echo "dbport = ${dbport:=6432}"
echo "dbname = ${dbname:=datacube}"
echo "##########################"
export module_dir agdc_env_module pyvariant variant dbhost dbport dbname

echoerr() { echo "$@" 1>&2; }

if [[ $# != 1 ]] || [[ "$1" == "--help" ]];
then
    echoerr 
    echoerr "Usage: $0 <version>"
    echoerr "Overriding any above variables as needed."
    echoerr "  eg. pyvariant=py3 variant=prod dbhost=130.56.244.225 agdc_env_module=agdc-py3-env/20161201 $0 1.1.17"
    exit 1
fi
export version="$1"

export agdc_module=agdc-${pyvariant}
export module_name=agdc-${pyvariant}-${variant}
export module_dest=${module_dir}/${module_name}/${version}
export module_description="AGDC ${variant} instance"

echo '# Packaging '$module_name' version '$version' to '$module_dest' #'
read -p "Continue? " -n 1 -r
echo    # (optional) move to a new line

function render {
    # User perl instead of envsubst so that we can complain loudly about missing vars
    perl -p -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : (print STDERR "Undefined: $&\n" and exit 1)/eg' < "$1" > "$2"
    #envsubst < "$1" > "$2"
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

