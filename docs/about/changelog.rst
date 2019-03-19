

.. _changelog:

===========
 Changelog
===========

2019-03-12
==========

Users are now required to join all projects containing data they wish to use. Before this change
all the DEA data was public to NCI users without any further steps.

See :ref:`data_access` for more details.

2018-02-28
==========


 * Rename module to `dea`. Most people should now run::

    module use /g/data/v10/public/modules
    module load dea

 * Include JupyterLab_ as an alternative to Jupyter Notebooks. To use, from a shell run::

      jupyter-lab

 * Include pre-release version 1.6 of Open Data Cube

 * Drop support for Python 2





.. _JupyterLab: https://blog.jupyter.org/jupyterlab-is-ready-for-users-5a6f039b8906
