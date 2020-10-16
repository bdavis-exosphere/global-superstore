select distinct
    product_id || '-' || product_name::varchar(255) as product_id,
    product_id as product_number,
    product_name::varchar(255) as product_name,
    substring(product_name, 1, position(' ', product_name)) as brand,
    sub_category as product_sub_category,
    category as product_category,
    hash(product_id, product_name, substring(product_name, 1, position(' ', product_name)), sub_category, category) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_product_master'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_product_master') }}
