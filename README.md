[![Build Status](https://travis-ci.org/GeoscienceAustralia/digitalearthau.svg?branch=develop)](https://travis-ci.org/GeoscienceAustralia/digitalearthau)

## Digital Earth Australia

A deployment of Open Data Cube.

This repository contains deployment, config and collection-management code specific
to the DEA instance of the Open Data Cube.

### Building NCI modules

See the modules [readme](modules/README.md)

### Creating a DEA-configured Data Cube

DEA has its own metadata types, so you must initialise using the config in this
repository rather that `datacube system init`. A command is available to do this:

    dea-system init
