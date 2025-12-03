{{ config(
    materialized = 'incremental',
    unique_key = ['product_id', 'warehouse_id', 'snapshot_date'],
    incremental_strategy = 'merge'
) }}

WITH inventory AS (
    SELECT
        inventory_id,
        product_id,
        warehouse_id,
        on_hand_qty,
        reserved_qty,
        on_order_qty,
        snapshot_date
    FROM {{ ref('stg_inventory') }}
    {% if is_incremental() %}
      WHERE snapshot_date >= (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}
),

product AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory,
        unit_of_measure
    FROM {{ ref('stg_product') }}
),

po AS (
    SELECT
        product_id,
        SUM(quantity) AS pending_order_qty,
        MIN(expected_delivery) AS next_delivery_date
    FROM {{ ref('stg_purchase_order') }}
    WHERE expected_delivery >= CURRENT_DATE()
    GROUP BY 1
),

joined AS (
    SELECT
        i.product_id,
        i.warehouse_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.unit_of_measure,
        i.on_hand_qty,
        i.reserved_qty,
        i.on_order_qty,
        COALESCE(po.pending_order_qty, 0) AS pending_order_qty,
        COALESCE(po.next_delivery_date, NULL) AS next_delivery_date,
        (i.on_hand_qty - i.reserved_qty) AS available_stock,
        i.snapshot_date
    FROM inventory i
    LEFT JOIN product p ON i.product_id = p.product_id
    LEFT JOIN po ON i.product_id = po.product_id
),

metrics AS (
    SELECT
        *,
        CASE
            WHEN available_stock <= 0 THEN 'OUT_OF_STOCK'
            WHEN available_stock < 10 THEN 'LOW_STOCK'
            WHEN reserved_qty > on_hand_qty THEN 'OVER_RESERVED'
            WHEN on_hand_qty > 1000 THEN 'OVERSTOCK'
            ELSE 'STABLE'
        END AS alert_status
    FROM joined
)

SELECT
    product_id,
    product_name,
    category,
    subcategory,
    warehouse_id,
    on_hand_qty,
    reserved_qty,
    available_stock,
    on_order_qty,
    pending_order_qty,
    next_delivery_date,
    alert_status,
    snapshot_date
FROM metrics
