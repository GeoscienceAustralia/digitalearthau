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



Updating the Default Version
----------------------------

Once a module has been tested and approved, it can be made the default.

Edit the `.version` file in the modulefiles directory.

Eg. For `dea-env` this is: :file:`/g/data/v10/public/modules/modulefiles/dea-env/.version`

Notifying the community
-----------------------

Notify the community of the release using the Datacube Central mailing list
The notifications are sent out using MailChimp_. You might need an invitation from the Geoscience
Australia account.

Create a campaign (possibly by replicating one of the existing ones) and change the notes.
You can send out a test mail to selected accounts before sending it out to the entire DEA
Beta Users list.

.. _MailChimp: https://www.mailchimp.com

Individual Modules
==================


Python "Environment" Module
---------------------------

The Python module contains all Data Cube dependencies and libraries but not the
Data Cube itself. See [environment.yaml](py-environment/environment.yaml)
for the list of packages.

The module version number is the current date in format ``YYYYMMDD``, as it is a snapshot
of all of our pip/conda dependencies on that date.


.. note::
  Loading the module might conflict with other python modules you have loaded.

  The module will prevent conflicts with locally installed python packages by changing ``PYTHONUSERBASE`` for each release;
  ``pip install --user ...`` will store packages under ``~/.digitalearthau``.

DEA Module
----------

The DEA module contains the Open Data Cube library, along with all other Geoscience Specific code. It is built against a
specific Python envionment module (ie. frozen to specific versions of each of our dependencies)

