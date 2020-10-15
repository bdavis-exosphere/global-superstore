select distinct
    order_priority as order_priority_id,
    order_priority as order_priority,
    hash(order_priority) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_pos_orders'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_pos_orders') }}
