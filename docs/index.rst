.. Digital Earth Australia documentation master file, created by
   sphinx-quickstart on Wed Jun  7 17:22:24 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. image:: _static/dea-logo-inline.svg
   :align: center
   :alt: Digital Earth Australia Logo

|
|

Digital Earth Australia User Guide
##################################

Digital Earth Australia is an analysis platform for satellite imagery and other Earth observations.

For more information on the project, see http://www.ga.gov.au/dea

Digital Earth Australia is currently in beta for users with
accounts on the National Computational Infrastructure (NCI).

Data can be viewed on `NationalMap <https://nationalmap.gov.au/>`_,
or accessed directly at https://data.dea.ga.gov.au (see :ref:`s3`).

Our OGC Web Service supporting WMS, WCS and some WPS functionality is `<https://ows.services.dea.ga.gov.au/>`_.

If you notice an error in this documentation, things that could be explained more clearly, or things that are missing,
please let us know by creating an Issue in the
`dea-notebooks Github repository <https://github.com/GeoscienceAustralia/dea-notebooks/issues>`_,
and list what you would like to see changed.

.. toctree::
   :maxdepth: 1
   :caption: Overview

   about/intro.rst
   about/glossary.rst
   about/changelog.rst

.. toctree::
   :maxdepth: 1
   :caption: Connect

   connect/account.rst
   connect/install.rst
   connect/get_help.rst
   connect/nci_basics.rst
   connect/jupyter.rst
   internal/other_modules.rst


.. toctree::
   :caption: User Guide
   :glob:

   notebooks/*/README


.. toctree::
   :maxdepth: 1
   :caption: Data Guide

   data/data.rst
   data/s3.rst
   data/s3-sftp.rst


.. toctree::
   :maxdepth: 1
   :caption: Developer Guide

   internal/new_product.rst
   internal/git_best_practice.rst
   internal/release.rst
   internal/modules.rst
   internal/collection_management.rst
   internal/requirements_met.ipynb


.. toctree::
  :caption: Indexes

  genindex

