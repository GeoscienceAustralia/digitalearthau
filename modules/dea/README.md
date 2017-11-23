## DEA Module packager

Run `./package-module.sh` to build a module

    Usage: ./package-module.sh <dea_tag/suffix> [<datacube_module>]

### Development build

To create a build of current develop:

    ./package-module.sh develop

or

    ./package-module.sh develop agdc-py3-prod/1.5.1


### Tagged build

Tags should be of the form `dea-YYYYMMDD`, but when specifying them to
this script, use ONLY the date portion.

eg.

    ./package-module 20171123

or

    ./package-module 20171123 agdc-py3-prod/1.5.3




(Note that tagged modules will be frozen: write permission is self-revoked on completion)

