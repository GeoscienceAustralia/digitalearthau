#%Module########################################################################
##
## ${package_name} modulefile
##

proc ModulesHelp { } {
        global version

        puts stderr "   ${package_description}"
        puts stderr "   Version ${version}"
}

set version       ${version}
set name          ${package_name}
set base          ${module_dir}

module-whatis   "${package_description} ${version}"

if { [ module-info mode load ] } {
    if { ! [is-loaded agdc-py3-env/${py3_env_version}] } {
        module load agdc-py3-env/${py3_env_version}
    }
    prepend-path PYTHONPATH ${module_dir}/${package_name}/${version}/lib/python3.5/site-packages
    prepend-path PATH ${module_dir}/${package_name}/${version}/bin

    setenv DATACUBE_CONFIG_PATH ${module_dir}/${package_name}/${version}/datacube.conf
    setenv LC_ALL en_AU.utf8
    setenv LANG C.UTF-8
}

if { [ module-info mode remove ] } {
    remove-path PYTHONPATH ${module_dir}/${package_name}/${version}/lib/python3.5/site-packages
    remove-path       PATH ${module_dir}/${package_name}/${version}/bin

    unsetenv DATACUBE_CONFIG_PATH
    module unload agdc-py3-env/${py3_env_version}
}

