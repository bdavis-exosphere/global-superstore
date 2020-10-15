select distinct
    case
        when market = region then region
        else market || ' - ' || region
    end::varchar(50) as customer_region_id,
    case
        when market = region then region
        else market || ' - ' || region
    end::varchar(50) as customer_region,
    market as customer_market,
    hash(case
            when market = region then region
            else market || ' - ' || region
         end, market) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_crm'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_crm') }}
