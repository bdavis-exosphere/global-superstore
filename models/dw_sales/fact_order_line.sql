select
    pos.row_id as order_line_id,
    pos.order_id,
    pos.order_date,
    pos.ship_date,
    pos.ship_mode as ship_mode_id,
    pos.order_priority as order_priority_id,
    pos.customer_id,
    nvl(pos.city,'Unknown') || '-' || nvl(pos.state,'Unknown') || '-' || nvl(pos.postal_code,'Unknown') as customer_geography_id,
    case
        when crm.market = crm.region then crm.region
        else crm.market || ' - ' || crm.region
    end::varchar(50) as customer_region_id,
    pos.product_id || '-' || pos.product_name as product_id,
    'ACTUAL'::varchar(20) as scenario_id,
    round(pos.sales,2)::decimal(18,2) as order_amount,
    pos.quantity::smallint as order_quantity,
    round(pos.discount,4)::decimal(8,4) as order_discount_percent,
    round(pos.profit,2)::decimal(18,2) as order_profit,
    round(pos.shipping_cost,2)::decimal(18,2) as order_shipping_cost,
    hash(pos.order_id, pos.order_date, pos.ship_date, pos.ship_mode, pos.order_priority, pos.customer_id, nvl(pos.city,'Unknown') || '-' || nvl(pos.state,'Unknown') || '-' || nvl(pos.postal_code,'Unknown'), pos.product_id, round(pos.sales,2),
        pos.quantity, round(pos.discount,4),  round(pos.profit,2), round(pos.shipping_cost,2)) as checksum,
    'demo_raw_zone.global_superstore.global_superstore_pos_orders'::varchar(255) as source_name,
    current_timestamp as dw_insert_ts,
    current_timestamp as dw_update_ts
from {{ source('raw_global_superstore', 'global_superstore_pos_orders') }} pos
join {{ source('raw_global_superstore', 'global_superstore_crm') }} crm
    on pos.customer_id = crm.customer_id
    and nvl(pos.city,'Unknown') = nvl(crm.city,'Unknown')
    and nvl(pos.state,'Unknown') = nvl(crm.state,'Unknown')
    and nvl(pos.postal_code,'Unknown') = nvl(crm.postal_code,'Unknown')
    and pos.country = crm.country
join demo_raw_zone.global_superstore.global_superstore_product_master pm
    on pos.product_id = pm.product_id
    and pos.product_name = pm.product_name
