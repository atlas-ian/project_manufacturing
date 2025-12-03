<<<<<<< HEAD
{{ config(
  materialized = 'incremental',
  unique_key = 'product_warehouse_snapshot'
) }}

/*
int_inventory_alerts.sql
- reads directly from source tables (use source(...) to point to manufacturing_db.staging raw tables)
- computes inventory health / reorder signals per product x warehouse x snapshot_date
- creates product_warehouse_snapshot unique key used by dbt incremental merge
*/

with
-- -----------------------
-- source / raw selects
-- -----------------------
inv as (
    select
        inventory_id,
        product_id,
        warehouse_id,
        on_hand_qty,
        on_order_qty,
        snapshot_date
    from {{ source('src','raw_inventory') }}
),

prod as (
    select
        product_id,
        product_name,
        supplier_id,
        category,
        unit_of_measure,
        created_ts
    from {{ source('src','raw_product') }}
),

-- -----------------------
-- incremental filter: only new snapshot dates
-- -----------------------
inv_new as (
    select *
    from inv

    {% if is_incremental() %}
      where snapshot_date > (
          select coalesce(max(snapshot_date), '1900-01-01') from {{ this }}
      )
    {% endif %}
),

-- -----------------------
-- join and basic aggregates (per product x warehouse x date)
-- -----------------------
joined as (
    select
        i.product_id,
        p.product_name,
        p.supplier_id,
        p.category,
        p.unit_of_measure,
        i.warehouse_id,
        i.snapshot_date,
        coalesce(i.on_hand_qty, 0) as on_hand_qty,
        coalesce(i.on_order_qty, 0) as on_order_qty,
        coalesce(i.on_hand_qty,0) + coalesce(i.on_order_qty,0) as total_stock,
        case
          when p.created_ts is not null and i.snapshot_date is not null
            then datediff('day', date_trunc('day', p.created_ts), i.snapshot_date)
          else null
        end as product_age_days
    from inv_new i
    left join prod p using (product_id)
),

-- -----------------------
-- business defaults and derived calculations
-- -----------------------
calc as (
    select
        -- unique key for incremental MERGE: product|warehouse|date
        product_id || '-' || warehouse_id || '-' || to_varchar(snapshot_date, 'YYYY-MM-DD') 
            as product_warehouse_snapshot,

        product_id,
        product_name,
        supplier_id,
        category,
        unit_of_measure,
        warehouse_id,
        snapshot_date,
        on_hand_qty,
        on_order_qty,
        total_stock,
        product_age_days,

        -- default lead time (days) for reorder calculation; replace with supplier lead-time when available
        7 as lead_time_days,

        -- placeholder avg daily usage (null). downstream models should replace this with computed usage.
        null::double precision as avg_daily_usage,

        -- safety stock: default to 10% of on_hand_qty if no usage data
        round( coalesce(on_hand_qty,0) * 0.10, 2) as safety_stock_default,

        -- reorder_point: lead_time * avg_daily_usage + safety_stock
        -- if avg_daily_usage is null, fallback to safety_stock_default * 2 (simple heuristic)
        case
          when avg_daily_usage is not null
            then round( lead_time_days * avg_daily_usage + round(coalesce(on_hand_qty,0) * 0.10,2), 2)
          else round( coalesce(on_hand_qty,0) * 0.10 * 2, 2 )
        end as reorder_point,

        -- days_of_supply = on_hand / avg_daily_usage (null if avg_daily_usage null)
        case
          when avg_daily_usage is not null and avg_daily_usage > 0
            then round(on_hand_qty / avg_daily_usage, 2)
          else null
        end as days_of_supply,

        -- simple reorder flag: true if total_stock <= reorder_point OR on_hand_qty == 0
        case
          when (coalesce(on_hand_qty,0) + coalesce(on_order_qty,0)) <= (
               case when avg_daily_usage is not null then (lead_time_days * avg_daily_usage + round(coalesce(on_hand_qty,0)*0.10,2))
                    else round(coalesce(on_hand_qty,0)*0.10*2,2)
               end
           ) then true
          when coalesce(on_hand_qty,0) = 0 then true
          else false
        end as reorder_flag,

        -- stock_status: out_of_stock / low / healthy / unknown
        case
          when coalesce(on_hand_qty,0) = 0 then 'out_of_stock'
          when avg_daily_usage is not null and avg_daily_usage > 0 and on_hand_qty / avg_daily_usage < 3 then 'low'
          when coalesce(on_hand_qty,0) <= coalesce(round(coalesce(on_hand_qty,0) * 0.10,2)*2,0) then 'low'
          when coalesce(on_hand_qty,0) > 0 and (coalesce(on_hand_qty,0) + coalesce(on_order_qty,0)) >= coalesce(round(coalesce(on_hand_qty,0) * 0.50,2),1) then 'healthy'
          else 'unknown'
        end as stock_status,

        -- simple fillable indicator: whether on_order_qty will cover shortfall (nullable when avg_daily_usage unknown)
        case
          when avg_daily_usage is null then null
          when on_order_qty >= (lead_time_days * avg_daily_usage) then true
          else false
        end as incoming_covers_leadtime,

        -- metric to help dashboards: pct_on_order = on_order / total_stock
        case when (coalesce(on_hand_qty,0) + coalesce(on_order_qty,0)) > 0
             then round( coalesce(on_order_qty,0) / (coalesce(on_hand_qty,0) + coalesce(on_order_qty,0)) * 100, 2)
             else 0
        end as pct_on_order

    from joined
)

select * from calc
=======
with products as (

    select
        cast(product_id as varchar) as product_id,
        product_name,
        
        -- Added these columns so they flow to the final table
        supplier_id,
        unit_of_measure,
        
        category,
        -- Handle potential null prices to avoid math errors later
        coalesce(price, 0) as unit_price_usd,
        weight_kg
    from {{ source('src', 'raw_product') }}

),

inventory as (

    select
        cast(product_id as varchar) as product_id,
        cast(warehouse_id as varchar) as warehouse_id,
        snapshot_date,
        -- Ensure we don't have null quantities
        coalesce(on_hand_qty, 0) as on_hand_qty,
        coalesce(on_order_qty, 0) as on_order_qty
    from {{ source('src', 'raw_inventory') }}

),

joined_data as (

    select
        inventory.snapshot_date,
        inventory.warehouse_id,
        products.product_id,
        products.product_name,
        
        -- Pass these through the join
        products.supplier_id,
        products.unit_of_measure,
        
        products.category,
        inventory.on_hand_qty,
        inventory.on_order_qty,
        products.unit_price_usd,
        products.weight_kg

    from inventory
    inner join products 
        on inventory.product_id = products.product_id

),

metrics as (

    select
        *,
        
        -- Total units available or incoming
        (on_hand_qty + on_order_qty) as total_supply_qty,
        
        -- Financial Value of current physical stock
        round(on_hand_qty * unit_price_usd, 2) as inventory_value_usd,
        
        -- Logistics: Total weight (useful for shipping estimates)
        round(on_hand_qty * weight_kg, 2) as total_weight_on_hand_kg

    from joined_data

),

final as (

    select
        snapshot_date,
        warehouse_id,
        product_id,
        product_name,
        
        -- Final Selection of the missing columns
        supplier_id,
        unit_of_measure,
        
        category,
        
        -- Core Metrics
        on_hand_qty,
        on_order_qty,
        total_supply_qty,
        inventory_value_usd,
        total_weight_on_hand_kg,
        
        -- ALERT LOGIC
        case
            when on_hand_qty = 0 then 'Critical: Stockout'
            when on_hand_qty < 100 then 'Warning: Low Stock'
            when on_hand_qty > 800 then 'Flag: Potential Overstock'
            else 'Healthy'
        end as inventory_status,

        -- Action Recommendation
        case
            when on_hand_qty = 0 and on_order_qty = 0 then 'Order Immediately'
            when on_hand_qty = 0 and on_order_qty > 0 then 'Expedite Shipment'
            when on_hand_qty < 100 and on_order_qty = 0 then 'Reorder Soon'
            when on_hand_qty > 800 then 'Stop Ordering / Promo'
            else 'No Action'
        end as recommended_action

    from metrics

)

select * from final
>>>>>>> eb53dc1215facab3ad23f1ba7fd942e96817cfc0
