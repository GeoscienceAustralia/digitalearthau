
# Environment module

The environment module contains all Data Cube dependencies and libraries but
not the Data Cube itself. See [environment.yaml](environment-module/environment.yaml) for the list of packages

### Creation

    cd environment-module
    ./package-module.sh --help
    ./package-module.sh --variant py3 --moduledir /g/data/v10/public/modules
    
This will create a new environment module in /g/data/v10/public/modules/agdc-py3-env/\<date\>.

### Use

    module load agdc-py3-env

### Notes

Loading the module might conflict with other python modules you have loaded.

The module will disable locally installed python packages to prevent conflicts by setting `PYTHONNOUSERSITE`

# Data Cube module

The data cube module contains the Data Cube code. It is built against a specific environment module.

### Creation

    cd datacube-module
    ./package-module.sh --help
    ./package-module.sh --env agdc-py3-env/21121221 --moduledir /g/data/v10/public/modules

This will create a new data cube module in /g/data/v10/public/modules/agdc-py3/\<version\>. Where `<version>` is the version of the latest code in [agdc-v2/develop](https://github.com/data-cube/agdc-v2/tree/develop) (e.g. 1.1.6+12.abcdefgh).

### Use

    module load agdc-py3
    
This will load `agdc-py3-env/21121221` and `agdc-py3/<version>` modules

# Database Configuration module

This module sets up data cube runtime configuration by setting `DATACUBE_CONFIG_PATH` environment variable.

The module requires `agdc-py3` module of matching version

### Creation

    cd database-module
    ./package-module.sh --help
    ./package-module.sh --variant py3 --name prod --version 1.1.6 --dbname datacube --dbhost 130.56.244.227 --dbport 6432 --moduledir /g/data/v10/public/modules
    
This will create a new configuration module in /g/data/v10/public/modules/agdc-py3/prod/1.1.6.

### Use

    module load agdc-py3-prod
    datacube system check

This will load `agdc-py3-env/21121221`, `agdc-py3/<version>` and `agdc-py3-prod/<version>` modules
