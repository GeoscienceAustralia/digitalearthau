# Australian Geoscience Data Cube Ingester Configuration and Utilities

## Module Creation

    ./package-module.sh --help
    ./package-module.sh --moduledir /g/data/v10/public/modules --agdc agdc-py3-prod
    
This will create a new ingester module in /g/data/v10/public/modules/agdc-ingester/\<version\>.

## Module Usage

    module load agdc-ingester
    datacube-ingester --help
