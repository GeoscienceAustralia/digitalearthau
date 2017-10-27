.. highlight:: console
.. internal_git_best_practice:

================================================
Collection Management
================================================

The DEA repository contains a set of tools for managing the collections on disk at NCI.

See their help commands for information:

.. code-block:: bash

    # Update the index, find problems.
    dea-sync --help
    dea-coherence --help
    dea-duplicates --help

    # Move datasets between disks safely and incrementally (keeping the index updated).
    dea-move --help

    # Trash archived datasets
    dea-clean --help

    # Submit a sync job to PBS
    dea-submit-sync --help

Many of these expect you to operate on collections:

================================================
Collections
================================================

The Open Data Cube keeps track of where individual datasets are, but not where datasets as a whole should be.

Knowing where they should be is currently done by this DEA repository, as it's specific to the NCI environment
(filesystem paths etc).

A collection defines datacube query arguments and folder patterns that should contain the same set of datasets,
as well as information on how to treat those datasets.

- Tools like the sync tool can then scan the index and disk and fix problems in both directions.

- The move tool can use it to "safely" perform moves, ensuring they stay in correct structure and that unrelated
files are not carried with them.

The set of collections are defined in `collections.py`_ which is currently acting like a config file.

.. _collections.py: https://github.com/GeoscienceAustralia/digitalearthau/blob/develop/digitalearthau/collections.py

Example collections:

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

