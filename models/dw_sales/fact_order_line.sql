select
    row_id as order_line_id,
    order_id,
    order_date,
    ship_date,
    ship_mode as ship_mode_id,
    order_priority as order_priority_id,
    customer_id,
    nvl(city,'Unknown') || '-' || nvl(state,'Unknown') || '-' || nvl(postal_code,'Unknown') as customer_geography_id,
    case
        when market = region then region
        else market || ' - ' || region
    end::varchar(50) as customer_region_id,
    product_id || '-' || product_name as product_id,
    'ACTUAL'::varchar(20) as scenario_id,
    round(sales,2)::decimal(18,2) as order_amount,
    quantity::smallint as order_quantity,
    round(discount,4)::decimal(8,4) as order_discount_percent,
    round(profit,2)::decimal(18,2) as order_profit,
    round(shipping_cost,2)::decimal(18,2) as order_shipping_cost,
    hash(order_id, order_date, ship_date, ship_mode, order_priority, customer_id, nvl(city,'Unknown') || '-' || nvl(state,'Unknown') || '-' || nvl(postal_code,'Unknown'), product_id, round(sales,2),
        quantity, round(discount,4),  round(profit,2), round(shipping_cost,2)) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_pos_orders'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_pos_orders') }}
