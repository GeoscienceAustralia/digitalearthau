.. highlight:: console

.. internal_release:

DEA Release Process
*******************

This document describes how to make a new release of Digital Earth Australia
onto the NCI computing environment.

Reasons for doing a new release
===============================

- New or updated libraries
- New Open Data Cube release
- Configuration changes need to be deployed
- Utilities in ``digitalearthau`` have been updated


Summary / Top Level Package
===========================

The ``package-release.sh`` script will build all modules for a given datacube version::

    ./package-release.sh 1.2.0

(Note that it's currently interactive: each section will ask for confirmation
of options before starting)

Using the module
----------------

.. code-block:: bash

    module load agdc-py3-prod
    datacube system check

This will load ``agdc-py3-env/<build_date>``, ``agdc-py3/<version>`` and
``agdc-py3-prod/<version>`` modules

Updating the Default Version
----------------------------

Once a module has been tested and approved, it can be made the default.

Edit the `.version` file in the modulefiles directory.

Eg. For agdc-py2-prod this is: `/g/data/v10/public/modules/modulefiles/agdc-py2-prod/.version`

Individual Modules
==================

You probably don't have to care about any of the commands below: they are all
run by the `./package-release.sh` command above.

But if you want to build modules individually, or know what they are, keep
reading.

Python "Environment" Module
===========================

The Python module contains all Data Cube dependencies and libraries but not the
Data Cube itself. See [environment.yaml](py-environment/environment.yaml)
for the list of packages.

The module version number is the current date in format ``YYYYMMDD``, as it is a snapshot
of all of our pip/conda dependencies on that date.

Creating the environment module
-------------------------------

Running this from Raijin is highly recommended, we've seen issues occur when run from VDI (ghost file locks):

.. code-block:: bash

    cd modules/py-environment/
    ./package-module.sh --help
    ./package-module.sh --variant py3 --moduledir /g/data/v10/public/modules

This will create a new environment module in `/g/data/v10/public/modules/agdc-py3-env/\<date\>`.

Use the environment module
--------------------------
::

    module load agdc-py3-env

Notes
-----
Loading the module might conflict with other python modules you have loaded.

The module will prevent conflicts with locally installed python packages by changing ``PYTHONUSERBASE`` for each release;
``pip install --user ...`` will store packages under ``~/.digitalearthau``.

Data Cube Module
================

The data cube module contains the Open Data Cube library. It is built against a
specific Python envionment module (ie. frozen to specific versions of each of
our dependencies)

Creating a Datacube Module
--------------------------
.. code-block:: bash

    cd modules/agdc
    ./package-module.sh --help
        Usage: ./package-module.sh --env agdc-py2-env/anaconda2-2.5.0 --moduledir /g/data/v10/public/modules --version 1.1.9
          Options:
            --env agdc_env_module     Python module to use.
            --moduledir module_dir    Destination module path.
            --version version         GitHub tag to use. ie. 'datacube-<version>'. Default is '<HEAD>'

    ./package-module.sh --env agdc-py3-env/21121221 --moduledir /g/data/v10/public/modules

This will create a new data cube module in
``/g/data/v10/public/modules/agdc-py3/<version>``. Where ``<version>`` is the
version of the latest code in
`agdc-v2/develop <https://github.com/data-cube/agdc-v2/tree/develop>`_ (e.g.  1.1.6+12.abcdefgh).

To specify a particular version, use the version number portion of the GitHub tag.
Specifying ``--version 1.1.9`` will use the `datacube-1.1.9 <https://github.com/data-cube/agdc-v2/tree/datacube-1.1.9>`_ tag.

Use the Data Cube Module
------------------------
::

    module load agdc-py3

This will load ``agdc-py3-env/21121221`` and ``agdc-py3/<version>`` modules

Instance Module
===============

This module combines a Data Cube module with specific config (prod, test, dev...)

It includes a config file, which it specifies by setting the
``DATACUBE_CONFIG_PATH`` environment variable.

The version number matches the datacube version.

Create Custom Instance
----------------------
.. code-block:: bash

    cd modules/agdc-instances
    ./package-instance-module.sh  --help

See the example and directions in the above help output.
