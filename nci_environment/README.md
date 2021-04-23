[![Build Status](https://travis-ci.org/GeoscienceAustralia/digitalearthau.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/digitalearthau)

# Introduction

These scripts are all about our deployment of code onto the NCI.

We are going to have have three modules, with date-based version numbers:

 1. A *Python Environment* module

    * Contains third party dependencies of all of the GA code, installed via
      a `conda` environment.

 2. A *DEA* module, which depends on the _environment module_:

    * [Open Data Cube Core](https://github.com/opendatacube/datacube-core/)

    * [EO Datasets](https://github.com/GeoscienceAustralia/eo-datasets/)

    * [Digital Earth AU](https://github.com/GeoscienceAustralia/digitalearthau/)

    * [Data Cube Stats](https://github.com/GeoscienceAustralia/datacube-stats/)

    * [Fractional Cover](https://github.com/GeoscienceAustralia/fc/)

    * Creates users accounts in the Production Database the first time it is
      loaded by a user.

    * A configuration file including environments for the available _Indexes_


 3. An *LPGS* module for Landsat Level 1 Processing. It depends on the _DEA_ module, and contains:

    * [IDL Functions](https://github.com/sixy6e/idl-functions/)

    * [EO Tools](https://github.com/GeoscienceAustralia/eo-tools/)

    * [GAIP/WAGL](https://github.com/GeoscienceAustralia/gaip/)

    * [GQA](https://github.com/GeoscienceAustralia/gqa/)

    * [GALPGS](https://github.com/jeremyh/galpgs/)


# User instructions

    module load dea
    datacube system check

This will load the latest version of `dea/<build_date>` module.

It will also load `dea-env/<build_date>` which contains all of the software
dependencies for using DEA.

## Notes

Loading these module might conflict with other python modules you have loaded.

The `dea-env` module will prevent conflicts with locally installed python packages by
changing `PYTHONUSERBASE` for each release;

    pip install --user <package_name>

will store packages under `~/.digitalearthau`.


It includes a config file, which it specifies by setting the
`DATACUBE_CONFIG_PATH` environment variable.

# Maintainer Instructions

Only run these scripts from Raijin. We've seen filesystem sync issues when
run from VDI.

    module load python3/3.6.2
    pip3 install --user pyyaml jinja2

## Building a new _Environment Module_

It requires python 3.6+ and pyyaml. Run the following on raijin at the NCI:

      $ module use /g/data/v10/public/modules/modulefiles/
      $ module load python3/3.6.2
      $ ./build_environment_module.py dea-env/modulespec.yaml

This will build a new environment module for today.

The module version number is the current date in format YYYYMMDD, as it is a snapshot
of all of our pip/conda dependencies on that date.

## Building a new _DEA Module_

A DEA module will specify one exact environment module.

    $ module use /g/data/v10/public/modules/modulefiles/
    $ module load python3/3.6.2
    $ ./build_environment_module.py dea/modulespec.yaml

## Updating the Default Version

Once a module has been tested and approved, it can be made the default.

Edit the `.version` file in the modulefiles directory.

Eg. For `dea` this is: `/g/data/v10/public/modules/modulefiles/dea/.version`


## Re-Building _dea-unstable_

    module load python3/3.6.2
    rm -rf /g/data/v10/public/modules/dea/unstable
    python3 build_environment_module.py dea_unstable/modulespec.yaml


## Archiving an old module

[TO DO]...
