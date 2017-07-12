[![Build Status](https://travis-ci.org/GeoscienceAustralia/digitalearthau.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/digitalearthau)

## Digital Earth Australia

A deployment of Open Data Cube.

This repository contains deployment, config and collection-management code specific
to the DEA instance of the Open Data Cube.

### Building NCI modules

See the modules [readme](modules/README.md)

### Creating a DEA-configured Data Cube

DEA has its own metadata types, so you must initialise a new datacube without
the default ones:

    datacube -v system init --no-default-types

Then add metadata types and products from this repository:

    datacube -v metadata_type add digitalearthau/config/metadata-types.yaml
    datacube -v product add digitalearthau/config/products/ls*_scenes.yaml
