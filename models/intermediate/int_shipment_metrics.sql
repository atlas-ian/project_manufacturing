with products as (

    select
        cast(product_id as varchar) as product_id,
        product_name,
        category,
        category as subcategory 
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
        -- Check if delivery_date exists in raw data
        delivery_date, 
        status as shipment_status

    from {{ source('src', 'raw_shipment') }}

),

joined_data as (

    select
        shipments.shipment_id,
        shipments.product_id,
        products.product_name,
        products.category,
        products.subcategory,
        shipments.quantity_shipped,
        shipments.ship_date,
        shipments.delivery_date,
        shipments.shipment_status,
        
        coalesce(production_agg.total_planned_quantity, 0) as total_planned_quantity,
        production_agg.latest_end_date

    from shipments
    left join products 
        on shipments.product_id = products.product_id
    left join production_agg
        on shipments.product_id = production_agg.product_id

),

metrics as (

    select
        *,
        
        -- Fulfillment Rate
        round(
            (quantity_shipped / nullif(total_planned_quantity, 0)) * 100,
        2) as fulfillment_rate_pct,

        latest_end_date as planned_completion_date,

        -- 1. Transit Time: How long did it take to get to the customer? (Real metric)
        datediff(day, ship_date, delivery_date) as transit_days,

        -- 2. Delay Days: Cap at 0. If negative, it implies 0 delay (Shipped from stock)
        greatest(0, datediff(day, latest_end_date, ship_date)) as delay_days,

        -- 3. Timing Status: Explains the context
        case 
            when ship_date < latest_end_date then 'Shipped from Inventory'
            when ship_date > latest_end_date then 'Production Lag'
            else 'Just-in-Time'
        end as shipment_timing_status

    from joined_data

)

select * from metrics