# Module Deployment on the NCI

This directory contains a starting point for more maintainable system for deploying datacube code
modules onto the NCI.

At the moment it can install the agdc-statistics module, but can hopefully be extended to setting up
all the other modules.

After loading `agdc-py3` and installing ansible to `$HOME` it can be run with:

    ansible-playbook -v -v -i "localhost," -c local install-stats-module.yml