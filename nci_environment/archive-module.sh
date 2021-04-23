#!/usr/bin/env bash
#
# Tar the given module (typically done to free up inodes)
#
# Usage example: ./archive-module.sh agdc-py3-env 20150223
#

set -eu

module_name="$1"
module_version="$2"

prefix="/g/data/v10/public/modules"

full_name="${module_name}/${module_version}"
module_path="${prefix}/${full_name}"
modulefile_path="${prefix}/modulefiles/${full_name}"

echo "${module_path}"
echo "${modulefile_path}"

[[ -e "${module_path}" ]] || ( echo "No module found ${full_name}"; exit 1)
[[ -e "${modulefile_path}" ]] || ( echo "No modulefile found called ${full_name}"; exit 1)

# Create tar with module and modulefile
tar_path="${module_path}.tar"
echo "Tarring to ${tar_path}"
time tar -cf "${tar_path}" "${module_path}" "${modulefile_path}"

# Remove originals.
echo "Cleaning up"
echo
set -x
rm -rf "${module_path}" "${modulefile_path}"
set +x
echo
echo "Done"
