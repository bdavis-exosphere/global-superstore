select distinct
    order_id,
    hash(order_id) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_pos_orders'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_pos_orders') }}
