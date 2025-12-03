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
