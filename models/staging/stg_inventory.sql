WITH source_data AS (
    SELECT
        INVENTORY_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        ON_HAND_QTY,
        
        ON_ORDER_QTY,
        SNAPSHOT_DATE
    FROM {{ source('src', 'raw_inventory') }}

)

SELECT * FROM source_data
