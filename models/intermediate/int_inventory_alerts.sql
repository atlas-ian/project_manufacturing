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
    from {{ ref('stg_products') }}

),

inventory as (

    select
        cast(product_id as varchar) as product_id,
        cast(warehouse_id as varchar) as warehouse_id,
        snapshot_date,
        -- Ensure we don't have null quantities
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
