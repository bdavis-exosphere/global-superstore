select distinct
    nvl(city,'UNKNOWN') || '-' || nvl(state,'UNKNOWN') || '-' || nvl(postal_code,'UNKNOWN') as customer_geography_id,
    city as customer_city,
    state as customer_state,
    nvl(postal_code, 'UNKNOWN') as customer_postal_code,
    country as customer_country,
    hash(city, state, postal_code, country) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_crm'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_crm') }}
