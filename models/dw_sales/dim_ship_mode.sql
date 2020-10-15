select distinct
    ship_mode as ship_mode_id,
    ship_mode as ship_mode,
    hash(ship_mode) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_pos_orders'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_pos_orders') }}
