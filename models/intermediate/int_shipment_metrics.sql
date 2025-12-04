{{ config(
    materialized='table'
) }}

with products as (
    select
        cast(product_id as varchar) as product_id,
        product_name,
        category
    from {{ source('src', 'raw_product') }}
),

production_agg as (
    select
        cast(product_id as varchar) as product_id,
        sum(planned_quantity) as total_planned_quantity,
        max(end_date) as latest_end_date
    from {{ source('src', 'raw_production_order') }}
    group by 1
),

shipments as (
    select
        cast(shipment_id as varchar) as shipment_id,
        cast(product_id as varchar) as product_id,
        cast(warehouse_id as varchar) as warehouse_id,
        quantity as quantity_shipped,
        shipment_date as ship_date,
        delivery_date,
        status as shipment_status
    from {{ source('src', 'raw_shipment') }}
),

joined_data as (
    select
        s.shipment_id,
        s.product_id,
        p.product_name,
        p.category,

        s.quantity_shipped,
        s.ship_date,
        s.delivery_date,
        s.shipment_status,
        coalesce(pa.total_planned_quantity, 0) as total_planned_quantity,
        pa.latest_end_date
    from shipments s
    left join products p
        on s.product_id = p.product_id
    left join production_agg pa
        on s.product_id = pa.product_id
),

metrics as (
    select
        *,
        -- Fulfillment rate using macro
        {{ fulfillment_rate_pct('quantity_shipped', 'total_planned_quantity') }} as fulfillment_rate_pct,

        latest_end_date as planned_completion_date,

        -- Transit time using macro
        {{ transit_days('ship_date', 'delivery_date') }} as transit_days,

        -- Delay days using macro
        {{ delay_days('latest_end_date', 'ship_date') }} as delay_days,

        -- Timing status using macro
        {{ shipment_timing_status('ship_date', 'latest_end_date') }} as shipment_timing_status

    from joined_data
)

select * from metrics
