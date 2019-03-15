select l.uri_body from agdc.dataset_location as l, agdc.dataset as d, agdc.dataset_type as t
where l.dataset_ref = d.id
and  d.dataset_type_ref = t.id
and t.name = :product_name
and l.archived is NULL
and d.archived is NULL
and l.added between :from_date::timestamp and :to_date::timestamp;
