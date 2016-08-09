
# Environment module

The environment module contains all Data Cube dependencies and libraries but
not the Data Cube itself. See [environment.yaml](environment-module/environment.yaml) for the list of packages

### Creation

    cd environment-module
    ./package-module.sh --help
    ./package-module.sh --variant py3 --moduledir /g/data/v10/public/modules
    
This will create a new environment module in /g/data/v10/public/modules/agdc-py3-env/|date|.

### Use

    module load agdc-py3-env

### Notes

Loading the module might conflict with other python modules you might have loaded.

The module will disable locally installed python packages to prevent conflicts by setting `PYTHONNOUSERSITE`

# Data Cube module

The data cube module contains the Data Cube code

It is built against a specific environment module (see the variables at the top
of the script)

### Creation

    cd datacube-module
    ./package-module.sh --help
    ./package-module.sh --env agdc-py3-env/21121221 --moduledir /g/data/v10/public/modules

This will create a new data cube module in /g/data/v10/public/modules/agdc-py3/|version|.

### Use

    module load agdc-py3

# Database Configuration module

This module sets up data cube runtime configuration by setting `DATACUBE_CONFIG_PATH` environment variable.

### Creation

    cd database-module
    ./package-module.sh --help
    ./package-module.sh --moduledir /g/data/v10/public/modules --variant py3 --name prod --version 1.1.6 --dbname datacube --dbhost 130.56.244.227 --dbport 6432
    
This will create a new configuration module in /g/data/v10/public/modules/agdc-py3/prod/1.1.6.

### Use

    module load agdc-py3-prod
    datacube system check
