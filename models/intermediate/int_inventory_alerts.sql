{{ config(materialized='table') }}

with products as (

    select
        cast(product_id as varchar) as product_id,
        product_name,
        supplier_id,
        unit_of_measure,
        category,
        coalesce(price, 0) as unit_price_usd,
        weight_kg
    from {{ ref('stg_products') }}

),

inventory as (

    select
        cast(product_id as varchar) as product_id,
        cast(warehouse_id as varchar) as warehouse_id,
        snapshot_date,
        coalesce(on_hand_qty, 0) as on_hand_qty,
        coalesce(on_order_qty, 0) as on_order_qty
    from {{ ref('stg_inventory') }}

),

joined_data as (

    select
        i.snapshot_date,
        i.warehouse_id,
        p.product_id,
        p.product_name,
        p.supplier_id,
        p.unit_of_measure,
        p.category,
        i.on_hand_qty,
        i.on_order_qty,
        p.unit_price_usd,
        p.weight_kg
    from inventory i
    inner join products p
        on i.product_id = p.product_id

),

metrics as (

    select
        *,
        (on_hand_qty + on_order_qty) as total_supply_qty,
        round(on_hand_qty * unit_price_usd, 2) as inventory_value_usd,
        round(on_hand_qty * weight_kg, 2) as total_weight_on_hand_kg
    from joined_data

),

final as (

    select
        snapshot_date,
        warehouse_id,
        product_id,
        product_name,
        supplier_id,
        unit_of_measure,
        category,
        on_hand_qty,
        on_order_qty,
        total_supply_qty,
        inventory_value_usd,
        total_weight_on_hand_kg,
        {{ inventory_status('on_hand_qty') }} as inventory_status,
        {{ inventory_recommended_action('on_hand_qty', 'on_order_qty') }} as recommended_action
    from metrics

)

select * from final
