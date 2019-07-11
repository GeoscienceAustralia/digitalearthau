------------------------------------------
-- Delete a Data Cube Product
------------------------------------------
--
-- Use from the command line with psql. eg:
--
-- psql -v product_name=ls8_fc_albers_test delete_odc_product.sql
--
--

\conninfo

set search_path to agdc;
--
-- COUNT NUMBER OF DATASETS OF EACH TYPE (including archived)
--

select
  count(*),
  t.name
from dataset
  left join dataset_type t on dataset.dataset_type_ref = t.id
group by t.name;

--
-- CHECK FOR LINEAGE RECORDS
--

-- Are there any datasets that are descendents of this product?
-- If so, they will need to be removed first!
select count(*)
from dataset_source
  left join dataset d on dataset_source.source_dataset_ref = d.id
where d.dataset_type_ref = (select id
                            from dataset_type
                            where dataset_type.name = :'product_name');

-- Are there any lineage records which need deleting?
-- These are the lineage history of the product we're deleting

select count(*)
from dataset_source
  left join dataset d on dataset_source.dataset_ref = d.id
where d.dataset_type_ref = (select id
                            from dataset_type
                            where dataset_type.name = :'product_name');

\prompt About to start deleting records, are you sure you want to continue? (Ctrl-C to abort.)
--
-- DELETE LINEAGE RECORDS
--
WITH datasets as (SELECT id
                  FROM dataset
                  where dataset.dataset_type_ref = (select id
                                                    FROM dataset_type
                                                    WHERE name = :'product_name'))
DELETE FROM dataset_source
USING datasets
where dataset_source.dataset_ref = datasets.id;


--
-- CHECK FOR LOCATION RECORDS
--
select count(*)
from dataset_location
  left join dataset d on dataset_location.dataset_ref = d.id
where d.dataset_type_ref = (select id
                            from dataset_type
                            where dataset_type.name = :'product_name');


WITH datasets as (SELECT id
                  FROM dataset
                  where dataset.dataset_type_ref = (select id
                                                    FROM dataset_type
                                                    WHERE name = :'product_name'))
select count(*)
from dataset_location, datasets
where dataset_location.dataset_ref  = datasets.id;


--
-- DELETE LOCATION RECORDS
--
WITH datasets as (SELECT id
                  FROM dataset
                  where dataset.dataset_type_ref = (select id
                                                    FROM dataset_type
                                                    WHERE name = :'product_name'))
DELETE FROM dataset_location
USING datasets
where dataset_location.dataset_ref = datasets.id;

--
-- DELETE DATASET RECORDS
--
DELETE FROM dataset
where dataset.dataset_type_ref = (select id
                            from dataset_type
                            where dataset_type.name = :'product_name');


--
-- FINALLY, DELETE THE PRODUCT
--
DELETE FROM dataset_type
where dataset_type.name = :'product_name';
