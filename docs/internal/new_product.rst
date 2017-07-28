.. highlight:: console
.. internal_new_product

=============================================
Creating New Product [for devs]
=============================================

Notes for adding new products to NCI datacube

.. warning ::
    This is still work in progress


Test Database Access
--------------------

Make sure you have access to test database server that allows creation of new
databases. Test database server is ``agdcdev-db.nci.org.au``. Configure your
``~/.pgpass``, add new line like this one. Typically you should be able to
re-use the same credentials as your production datacube.

::

    agdcdev-db.nci.org.au:6432:*:<your user name>:<your random password>

Verify that things worked for you
``psql -h agdcdev-db.nci.org.au -p 6432 datacube -l``,
you should get a list of databases currently present.


Preparing New Database
----------------------

It's best to create an empty database for testing new product. Test database
should contain same metadata and products as the main database. Typically new
product is going to be derived from existing products in the main database and
since we track dataset lineage we need to have the same products in the test
database as in the main.

First create empty database

.. code-block:: bash

    mk_db_sql () {
       local db_name=${1}
       cat <<EOF
    CREATE DATABASE ${db_name}
    WITH
    OWNER = agdc_admin
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_AU.UTF-8'
    LC_CTYPE = 'en_AU.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

    GRANT ALL ON DATABASE ${db_name} TO agdc_admin;
    GRANT CONNECT, TEMPORARY ON DATABASE ${db_name} TO PUBLIC;
    GRANT ALL ON DATABASE ${db_name} TO test;
    ALTER DATABASE ${db_name} SET search_path TO "\$user", public, agdc;

    EOF
    }

    DB_NAME="${USER}_dev" # change to your liking
    mk_db_sql ${DB_NAME} | psql -h agdcdev-db.nci.org.au -p 6432 datacube


Create datacube config file that uses new database

.. code-block:: bash

    mk_dev_config () {
        local db_name=$1
        local f_name=${2-"${db_name}.conf"}
        cat > ${f_name} <<EOF
    [datacube]
    db_hostname: agdcdev-db.nci.org.au
    db_port: 6432
    db_database: ${db_name}
    EOF
    }

    mk_dev_config ${DB_NAME}


Tell datacube to use dev config via environment variable

.. code-block:: bash

    export DATACUBE_CONFIG_PATH=$(readlink -f ${DB_NAME}.conf)

To check this worked run ``datacube system check`` at this point system check
should fail as database is completely empty.


.. note::

   For now lets assume that exported list of products and metadata is maintained
   in a well-known location and is always up-to-date with data contained in the
   main database. Currently we will use data extracted by Damien (``dra547`` on
   NCI.)


.. code-block:: bash

    METADATA_TYPES="/g/data/u46/users/dra547/all_prod_metadata_types.yaml"
    PRODUCTS="/g/data/u46/users/dra547/all_prod_products.yaml"

    # Create and populate database
    datacube system init --no-default-types
    datacube metadata_type add "${METADATA_TYPES}"
    datacube product add "${PRODUCTS}"

To verify run system check and product list

.. code-block:: bash

    datacube system check
    datacube product list


Adding new product
------------------

Products are described using yaml. Things you need to customize

#. Product name
#. Product description
#. Product type
#. Metadata type (use ``eo``) and expected metadata fields
#. Configure Measurements (data variables): names, types, nodata values, units

Note about ``product_type`` it's really just a label/tag, an example would be
``nbar`` or ``pqa``, it is important that datasets are labeled with the exact
same ``product_type``. For ``metadata_type`` use ``eo`` and a template shown
below, see `Gotchas`_ section for more info.

.. code-block:: yaml

    name: <product_name>
    description: <free form description>

    # If unsure use eo
    # platform/instrument are optional, copy from source product
    # or omit if combining products from multiple platforms
    metadata_type: eo
    metadata:
      product_type: <product_type>
      format:
        name: NetCDF
      platform:
        code: <e.g. LANDSAT_5>
      instrument:
        name: <e.g. TM>

    measurements:
      # Repeat for all variables
      - name: <variable_name>
        dtype: <uint16|uint32|float|...>
        nodata: <optional, needed for integer types usually, -9999 for example>
        units: '1' # Measurement units like "meters", use '1' if unit-less

    # Typical settings, usually no need to change.
    # For derived products usually good idea to copy source product settings
    # for tile_size and resolution
    storage:
      driver: NetCDFCF
      crs: EPSG:3577
      tile_size:
        x: 100000.0
        y: 100000.0
      resolution:
        x: 25
        y: -25
      chunking:
        x: 200
        y: 200
        time: 1
      dimension_order: [time, y, x]


.. code-block:: bash

    NEW_PRODUCT_FILE="</path/to/product_file.yaml>"
    datacube product add ${NEW_PRODUCT_FILE}

    # Verify it was added
    datacube product list
    datacube product show "<product_name>"



Indexing data
-------------

For the purpose of this document we are going to assume that product datasets
were generated by the datacube tools and so they are

#. In NetCDF format
#. Contain ``dataset`` description yaml document embedded in them

To index datasets into the database just run ``datacube dataset add <files>``

Test it first with a single file without modifying database


.. code-block:: bash

    datacube dataset add --dry-run "<path_to_file/some_dataset.nc>"

If this succeeds add all files to index

.. code-block:: bash

    #flat directory hierarchy
    datacube dataset add path_to_datasets/*nc

    # OR if you have extra layer for tiles
    datacube dataset add path_to_datasets/**/*nc

    # OR for more complex hierarchies
    find path_to_datasets -name "*nc" | xargs datacube dataset add


Gotchas
-------

Most likely things won't go smoothly on the first try. This section addresses
some of the more common problem you are likely to experience.

First off these commands will help you debug various problems

.. code-block:: bash

    # view info about a NetCDF file
    ncdump -h -s "<dataset_file.nc>"
    gdalinfo "<dataset_file.nc>"

    # view NetCDF file
    ncview "<dataset_file.nc>"

    # view metadata embedded in a NetCDF file
    ncks -V -C -v dataset "<dataset_file.nc"> | less

    # check if datacube uses the right database
    datacube system check


Indexing fails
^^^^^^^^^^^^^^

Most likely indexing process failed with ``No matching Product found for ...``
error message.

#. Make sure ``metadata`` section in the product definition matches metadata
   embedded in the NetCDF file.
#. Make sure you are still using dev database
#. Make sure you have added product successfully


Role of Metadata
^^^^^^^^^^^^^^^^

Each dataset has a metadata document attached to it, for files produced by the
datacube tools this metadata is automatically generated. See `Metadata
<http://datacube-core.readthedocs.io/en/latest/ops/config.html#dataset-metadata-document>`_
documentation online for a detailed overview.

Metadata embedded inside the NetCDF file is used during indexing process for two purposes

#. To associate dataset to a product
#. To populate database search fields (lat/lon, time etc.)

When creating product description it is important that ``metadata`` section is
filled in a way consistent with the metadata present in data files. Usually this
means getting ``product_type`` right. If your new product is based on data from
one sensor at a time, then you should fill ``metadata.platform`` and
``metadata.instrument``, failing that you won't be able to create similar
products for other sensors and still re-use ``product_type``. If your product
combines data from multiple sensors/platforms then you should omit
platform/instrument from metadata section.


Reviewing Results
-----------------

#. Create Virtual Raster files
#. Create overview files for zoomed out view
#. Review in QGIS

We don't have generic tools for generating overviews, but here is an example
script for PQ stats. Customise for your needs:

#. Field names
#. Glob for file names and grouping by time

.. code-block:: bash

    nc_ls () {
        glob=$1
        field=$2
        for f in $(ls $glob); do
            echo "NETCDF:${f}:${field}"
        done
    }

    mk_overviews_ls_pq () {
        P="${1-'.'}"
        Y="${2-2014}"

        VARS="clear_observation_count total_observation_count"
        glob="LS_PQ_COUNT/**/LS_PQ_COUNT_3577_*_${Y}*nc"

        pushd "${P}"
        for var in $VARS; do
            nc_ls "${glob}" "${var}" | xargs gdalbuildvrt "${var}_${Y}.vrt"
            gdaladdo -r average ${var}_${Y}.vrt 16 32 64 128 256
        done
        popd
    }

    mk_overviews_ls_pq /g/data/u46/users/kk7182/PQ/ 2014

Create empty QGIS project and add generated ``*.vrt`` files to it. In the menu
select ``Layer> Add from Layer Definition File...`` navigate to
``/g/data/u46/users/kk7182/public/qgis/`` and select ``au.qlr``. This will add
vector layer that makes analysis easier.

Appendix
--------

.. literalinclude:: datacube_helpers.sh
   :language: bash
