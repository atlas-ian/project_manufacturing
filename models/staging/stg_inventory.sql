WITH source_data AS (
    SELECT
        INVENTORY_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        ON_HAND_QTY,
        RESERVED_QTY,
        ON_ORDER_QTY,
        SNAPSHOT_DATE
    FROM {{ source('src', 'raw_inventory') }}

),

cleaned AS (

    SELECT
        TRIM(INVENTORY_ID) AS inventory_id,
        TRIM(PRODUCT_ID) AS product_id,
        TRIM(WAREHOUSE_ID) AS warehouse_id,
        COALESCE(ON_HAND_QTY, 0) AS on_hand_qty,
        COALESCE(RESERVED_QTY, 0) AS reserved_qty,
        COALESCE(ON_ORDER_QTY, 0) AS on_order_qty,
        CAST(SNAPSHOT_DATE AS DATE) AS snapshot_date,
        CURRENT_TIMESTAMP() AS record_loaded_ts
    FROM source_data
)

SELECT * FROM cleaned
