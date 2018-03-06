[![Build Status](https://travis-ci.org/GeoscienceAustralia/digitalearthau.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/digitalearthau)

# Introduction

These scripts are all about our deployment of code onto the NCI.

We are going to have have three modules, with date-based version numbers:

 1. A *Python Environment* module
    * Contains third party dependencies of all of the GA code, installed via
      a `conda` environment.
 2. A *DEA* module, which depends on the _environment module_ and contains:
    * A script for initial user creation in the Production Database
    * A configuration file including environments for the available _Indexes_
    * [Open Data Cube Core](https://github.com/opendatacube/datacube-core/)
    * [Data Cube Stats](https://github.com/GeoscienceAustralia/datacube-stats/)
    * [Fractional Cover](https://github.com/GeoscienceAustralia/fc/)
    * [EO Datasets](https://github.com/GeoscienceAustralia/eo-datasets/)
 3. An *LPGS* module, which depends on a _DEA_ module, and contains:
    * [IDL Functions](https://github.com/sixy6e/idl-functions/)
    * [EO Tools](https://github.com/GeoscienceAustralia/eo-tools/)
    * [GAIP/WAGL](https://github.com/GeoscienceAustralia/gaip/)
    * [GQA](https://github.com/GeoscienceAustralia/gqa/)
    * [GALPGS](https://github.com/jeremyh/galpgs/)

The `package-release.sh` script will build all modules for a given datacube version:

    ./package-release.sh 1.2.0

Note that it's an interactive script: each section will ask for confirmation
of options before starting.

### Usage

    module load dea
    datacube system check

This will load `dea-env/<build_date>` and `dea/<build_date>` modules.

# Update the Default Version

Once a module has been tested and approved, it can be made the default.

Edit the `.version` file in the modulefiles directory.

Eg. For `dea` this is: `/g/data/v10/public/modules/modulefiles/dea/.version`

# Individual Modules

You probably don't have to care about any of the commands below: they are all
run by the `./package-release.sh` command above.

But if you want to build modules individually, or know what they are, keep
reading.

## Python "Environment" Module

The Python module contains all Data Cube dependencies and libraries but not the
Data Cube itself. See [environment.yaml](py-environment/environment.yaml)
for the list of packages.

The module version number is the current date in format YYYYMMDD, as it is a snapshot
of all of our pip/conda dependencies on that date.

### Creation

Running this from Raijin is highly recommended as we've seen some issues come up when ran from VDI (ghost file locks).

    cd modules/py-environment/
    ./package-module.sh --help
    ./package-module.sh --variant py3 --moduledir /g/data/v10/public/modules

This will create a new environment module in `/g/data/v10/public/modules/agdc-py3-env/\<date\>`.

### Use

    module load agdc-py3-env

### Notes

Loading the module might conflict with other python modules you have loaded.

The module will prevent conflicts with locally installed python packages by changing `PYTHONUSERBASE` for each release;
`pip install --user ...` will store packages under `~/.digitalearthau`.

## Data Cube Module

The data cube module contains the Open Data Cube library. It is built against a
specific Python envionment module (ie. frozen to specific versions of each of
our dependencies)

### Creation

    cd modules/agdc
    ./package-module.sh --help
        Usage: ./package-module.sh --env agdc-py2-env/anaconda2-2.5.0 --moduledir /g/data/v10/public/modules --version 1.1.9
          Options:
            --env agdc_env_module     Python module to use.
            --moduledir module_dir    Destination module path.
            --version version         GitHub tag to use. ie. 'datacube-<version>'. Default is '<HEAD>'

    ./package-module.sh --env agdc-py3-env/21121221 --moduledir /g/data/v10/public/modules

This will create a new data cube module in
`/g/data/v10/public/modules/agdc-py3/<version>`. Where `<version>` is the
version of the latest code in
[agdc-v2/develop](https://github.com/data-cube/agdc-v2/tree/develop) (e.g.  1.1.6+12.abcdefgh).

To specify a particular version, use the version number portion of the GitHub tag.
Specifying `--version 1.1.9` will use the [datacube-1.1.9](https://github.com/data-cube/agdc-v2/tree/datacube-1.1.9) tag.

### Use

    module load agdc-py3

This will load `agdc-py3-env/21121221` and `agdc-py3/<version>` modules

## Instance Module

This module combines a Data Cube module with specific config (prod, test, dev...)

It includes a config file, which it specifies by setting the
`DATACUBE_CONFIG_PATH` environment variable.

The version number matches the datacube version.

### Create Custon Instance.

    cd modules/agdc-instances
    ./package-instance-module.sh  --help

See the example and directions in the above help output.


