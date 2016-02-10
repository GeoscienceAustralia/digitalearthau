
# Environment module

The environment module contains all Data Cube dependencies and libraries but
not the Data Cube itself.

It's useful for development (when working from a Git checkout)


### Creation

You probably want to edit the script before running it (eg, use a test
modulepath initially). Feel free to make it more reusable.

    cd environment-module
    ./package-module.sh

### Use

    module load datacube-py2-env

# Data Cube module (demo environment)

The data cube module contains the Data Cube and config information (currently
for the demo environment)

It is built against a specific environment module (see the variables at the top
of the script)

### Creation

You probably want to edit the script before running it (eg, use a test
modulepath initially). Feel free to make it more reusable.

    cd datacube-module
    ./package-module.sh /path/to/agdc-v2-checkout

### Use

    module load datacube-demo-env

