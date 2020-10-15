select distinct
    customer_id,  --pk
    customer_name,
    segment as customer_segment,
    hash(customer_name, customer_segment) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_crm'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_crm') }}
