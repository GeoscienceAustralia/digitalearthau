# DEA Product & Ingestion configuration

## Creating a DEA-configured Data Cube

DEA has it's own metadata types, so you must initialise a new datacube without the default ones:

    datacube -v system init --no-default-types

Then add metadata types and products from this directory:

    datacube -v metadata_type add metadata-types.yaml
    datacube -v product add products/ls*_scenes.yaml

## Ingester Module Creation

    ./package-module.sh --help
    ./package-module.sh --moduledir /g/data/v10/public/modules --agdc agdc-py3-prod

This will create a new ingester module in /g/data/v10/public/modules/agdc-ingester/\<version\>.

## Ingester Module Usage

    module load agdc-ingester
    datacube-ingester --help
