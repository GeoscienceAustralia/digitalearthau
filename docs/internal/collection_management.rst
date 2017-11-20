.. highlight:: console
.. internal_git_best_practice:

================================================
Collection Management
================================================

The DEA repository contains a set of tools for managing the collections on disk at NCI.

See their help commands for specific information:

.. code-block:: bash

    # Update the index, find problems with datasets.
    dea-sync --help
    dea-coherence --help
    dea-duplicates --help

    # Move datasets between disks safely and incrementally (updating the index).
    dea-move --help

    # Trash archived datasets
    dea-clean --help

    # Submit a sync job to PBS
    dea-submit-sync --help

Note that many of these operate on collections, not products. To perform a move or sync on a new product you
may first need add it as a collection.

Defining Collections
------------------------------------------------

The Open Data Cube core keeps track of where individual datasets are in a product, but not where datasets as a
whole should be (such as which filesystems).

Knowing "where they should be" is currently handled in this DEA repository as the list of collections.

A collection defines:

- datacube query arguments and folder patterns that should contain the same set of datasets. The sync tool, for
  example, can then iterate the two to find mismatches in both directions.

- how datasets in the collection should be treated: is an unindexed file found on disk corrupt, or newly arrived?

The set of NCI DEA collections is currently in `collections.py`_.

.. _collections.py: https://github.com/GeoscienceAustralia/digitalearthau/blob/develop/digitalearthau/collections.py

Examples:

.. code-block:: python

    scene_collection(
            name='ls8_level1_scene',
            query={'product': ['ls8_level1_scene', 'ls8_level1_oli_scene']},
            file_patterns=[
                '/g/data/v10/reprocess/ls8/level1/[0-9][0-9][0-9][0-9]/[0-9][0-9]/LS*/ga-metadata.yaml',
            ],
        ),

    # Telemetry collection
    Collection(
            name='telemetry',
            query={'metadata_type': 'telemetry'},
            file_patterns=(
                '/g/data/v10/repackaged/rawdata/0/[0-9][0-9][0-9][0-9]/[0-9][0-9]/*/ga-metadata.yaml',
            ),
            # If something is archived, how many days before we can delete it? None means never
            delete_archived_after_days=None,
            # Who do we trust in a sync if there's a mismatch?
            trust=Trust.disk,
     )

Command line tools
------------------

Update the index, find problems with datasets.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. click:: digitalearthau.sync:cli
   :prog: dea-sync
   :show-nested:

.. click:: digitalearthau.coherence:main
  :prog: dea-coherence
  :show-nested:

.. click:: digitalearthau.duplicates:main
  :prog: dea-duplicates
  :show-nested:


Move datasets
^^^^^^^^^^^^^

Move datasets between disks safely and incrementally (updating the index).

.. click:: digitalearthau.move:cli
   :prog: dea-move
   :show-nested:

Trash archived datasets
^^^^^^^^^^^^^^^^^^^^^^^

.. click:: digitalearthau.cleanup:main
    :prog: dea-clean
    :show-nested:

Submit a sync job to PBS
^^^^^^^^^^^^^^^^^^^^^^^^

.. click:: digitalearthau.sync.submit_job:main
   :prog: dea-submit-sync
   :show-nested:


Initialise a database with DEA products
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. click:: digitalearthau.system:cli
   :prog: dea-system
   :show-nested:

..
    .. click:: digitalearthau.harvest.iso19115:main
        :prog: dea-harvest
        :show-nested:


    .. click:: digitalearthau.submit.ingest:cli
        :prog: dea-submit-ingest
        :show-nested:

    .. click:: digitalearthau.submit.ncmler:cli
        :prog: dea-submit-ncmler
        :show-nested:



