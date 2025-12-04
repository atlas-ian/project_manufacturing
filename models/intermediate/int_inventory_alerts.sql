{{ config(
    materialized='table'
) }}

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
        inventory.snapshot_date,
        inventory.warehouse_id,
        products.product_id,
        products.product_name,
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
        round(on_hand_qty * weight_kg, 2) as total_weight_on_hand_kg,
        -- Inventory status using macro
        {{ inventory_status('on_hand_qty') }} as inventory_status,
        -- Recommended action using macro
        {{ inventory_recommended_action('on_hand_qty', 'on_order_qty') }} as recommended_action

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
        inventory_status,
        recommended_action

    from metrics

)

select * from final
