{{ config(
    materialized='table',
    unique_key='INVENTORY_SK'
) }}

WITH source_data AS (
    SELECT 
        INVENTORY_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        ON_HAND_QTY,
        ON_ORDER_QTY,
        SNAPSHOT_DATE
    FROM {{ ref('stg_inventory') }}
),

logic_layer AS (
    SELECT
        s.INVENTORY_ID,
        s.WAREHOUSE_ID,
        s.SNAPSHOT_DATE,
        s.ON_HAND_QTY,
        s.ON_ORDER_QTY,
        
        -- High Value Metric (Pipeline)
        (s.ON_HAND_QTY + s.ON_ORDER_QTY) AS TOTAL_PIPELINE_QTY,

        -- Lookup Surrogate Key from Product Dimension
        COALESCE(p.product_key, '-1') AS PRODUCT_SK

    FROM source_data s
    -- Join on the Business Key (ID) to find the Surrogate Key (MD5 String)
    LEFT JOIN {{ ref('dim_products') }} p 
        ON CAST(s.PRODUCT_ID AS INT) = p.source_product_id
),

final_fact AS (
    SELECT
        -- 1. Primary Key
        ABS(HASH(INVENTORY_ID || TO_CHAR(SNAPSHOT_DATE, 'YYYYMMDD'))) AS INVENTORY_SK,

        -- 2. Foreign Keys
        PRODUCT_SK, 
        ABS(HASH(CAST(WAREHOUSE_ID AS INT))) AS WAREHOUSE_SK,
        TO_NUMBER(TO_CHAR(SNAPSHOT_DATE, 'YYYYMMDD')) AS SNAPSHOT_DATE_SK,

        -- 3. Metrics
        CAST(ON_HAND_QTY AS NUMBER) AS ON_HAND_QTY,
        CAST(ON_ORDER_QTY AS NUMBER) AS ON_ORDER_QTY,
        CAST(TOTAL_PIPELINE_QTY AS NUMBER) AS TOTAL_PIPELINE_QTY

    FROM logic_layer
)

SELECT * FROM final_fact