## DEA Module packager

Run `./package-module.sh` to build a module

    Usage: ./package-module.sh <dea_head> [<datacube_module]

To create a build of current develop:

    ./package-module.sh develop agdc-py3-dev/1.5.1

or

    ./package-module.sh develop agdc-py3-prod/1.5.1

To build a module for tag `dea-2017.1` against a specific agdc module:

    ./package-module.sh 2017.1 agdc-py3-prod/1.5.1

(Note that tagged modules will be frozen: write permission is self-revoked on completion)

