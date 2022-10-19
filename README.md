[![Main](https://github.com/GeoscienceAustralia/digitalearthau/actions/workflows/main.yml/badge.svg)](https://github.com/GeoscienceAustralia/digitalearthau/actions/workflows/main.yml)

## Digital Earth Australia

A deployment of Open Data Cube.

This repository contains deployment, config and collection-management code specific
to the DEA instance of the Open Data Cube.

### Building NCI modules

This has been removed.

### Creating a DEA-configured Data Cube

DEA has its own metadata types, so you must initialise using the config in this
repository rather than `datacube system init`. A command is available to do this:

    dea-system init
