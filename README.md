
# Modules
## Environment module

The environment module contains all Data Cube dependencies and libraries but
not the Data Cube itself. See [environment.yaml](environment-module/environment.yaml) for the list of packages

### Creation

Running this from Raijin is highly recommended as we've seen some issues come up when ran from VDI (ghost file locks).

    cd modules/py-environment/
    ./package-module.sh --help
    ./package-module.sh --variant py3 --moduledir /g/data/v10/public/modules
    
This will create a new environment module in /g/data/v10/public/modules/agdc-py3-env/\<date\>.

### Use

    module load agdc-py3-env

### Notes

Loading the module might conflict with other python modules you have loaded.

The module will disable locally installed python packages to prevent conflicts by setting `PYTHONNOUSERSITE`

## Data Cube module

The data cube module contains the Data Cube code. It is built against a specific environment module.

### Creation

    cd modules/agdc
    ./package-module.sh --help
        Usage: ./package-module.sh --env agdc-py2-env/anaconda2-2.5.0 --moduledir /g/data/v10/public/modules --version 1.1.9
          Options:
            --env agdc_env_module     Python module to use.
            --moduledir module_dir    Destination module path.
            --version version         GitHub tag to use. ie. 'datacube-<version>'. Default is '<HEAD>'

    ./package-module.sh --env agdc-py3-env/21121221 --moduledir /g/data/v10/public/modules

This will create a new data cube module in /g/data/v10/public/modules/agdc-py3/\<version\>. Where `<version>` is the version of the latest code in [agdc-v2/develop](https://github.com/data-cube/agdc-v2/tree/develop) (e.g. 1.1.6+12.abcdefgh).

To specify a particular version, use the version number portion of the GitHub tag.
Specifying `--version 1.1.9` will use the [datacube-1.1.9](https://github.com/data-cube/agdc-v2/tree/datacube-1.1.9) tag.

### Use

    module load agdc-py3
    
This will load `agdc-py3-env/21121221` and `agdc-py3/<version>` modules

## Instance configuration module

This module sets up data cube runtime configuration by setting `DATACUBE_CONFIG_PATH` environment variable.

The module requires `agdc-py3` module of matching version

### Create all instances

    cd modules/agdc-instances
    ./package-all-instances.sh 1.1.17
    
This will interactively create a module for each GA instance at NCI, confirming each one before creation.

### Create custon instance.

    cd modules/agdc-instances
    ./package-instance-module.sh  --help

See the example and directions in the above help output.

### Update default version

Update `/g/data/v10/public/modules/modulefiles/agdc-py3/prod/.version` to make the last version the default one.

### Use

    module load agdc-py3-prod
    datacube system check

This will load `agdc-py3-env/21121221`, `agdc-py3/<version>` and `agdc-py3-prod/<version>` modules

