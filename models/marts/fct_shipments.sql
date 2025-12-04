{{ config(
    unique_key = 'SHIPMENT_SK',
    on_schema_change = 'sync_all_columns'
) }}


WITH source_data AS (
    SELECT
        SHIPMENT_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        QUANTITY,
        SHIPMENT_DATE,
        TRY_TO_DATE(DELIVERY_DATE) AS DELIVERY_DATE,
        UPPER(TRIM(STATUS)) AS STATUS
    FROM {{ source('src', 'stg_shipment') }}

),

product_join AS (
    SELECT
        s.*,
        COALESCE(p.product_key, '-1') AS PRODUCT_SK
    FROM source_data s
    LEFT JOIN {{ ref('dim_products') }} p
        ON CAST(s.PRODUCT_ID AS INT) = p.source_product_id
),


logic_layer AS (
    SELECT
        *,
        -- Date SKs using YYYYMMDD pattern
        TO_NUMBER(TO_CHAR(SHIPMENT_DATE, 'YYYYMMDD')) AS SHIPMENT_DATE_SK,
        CASE WHEN DELIVERY_DATE IS NOT NULL
            THEN TO_NUMBER(TO_CHAR(DELIVERY_DATE, 'YYYYMMDD'))
        END AS DELIVERY_DATE_SK,

        -- Delay days
        CASE 
            WHEN DELIVERY_DATE IS NOT NULL 
            THEN DATEDIFF('day', SHIPMENT_DATE, DELIVERY_DATE)
        END AS SHIPMENT_DELAY_DAYS,

        -- On-time = delay <= 0
        CASE
            WHEN DELIVERY_DATE IS NOT NULL AND DATEDIFF('day', SHIPMENT_DATE, DELIVERY_DATE) <= 0
            THEN 1 ELSE 0
        END AS ON_TIME_FLAG,

        -- Warehouse surrogate key
        ABS(HASH(CAST(WAREHOUSE_ID AS VARCHAR))) AS WAREHOUSE_SK

    FROM product_join
),


final_fact AS (
    SELECT
        -- PRIMARY KEY: Surrogate SK
        ABS(HASH(SHIPMENT_ID || TO_CHAR(SHIPMENT_DATE, 'YYYYMMDD'))) AS SHIPMENT_SK,

        -- FOREIGN KEYS
        PRODUCT_SK,
        WAREHOUSE_SK,
        SHIPMENT_DATE_SK,
        DELIVERY_DATE_SK,

        -- BUSINESS DATA
        SHIPMENT_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        STATUS,
        QUANTITY,
        SHIPMENT_DATE,
        DELIVERY_DATE,

        -- METRICS
        SHIPMENT_DELAY_DAYS,
        ON_TIME_FLAG

    FROM logic_layer
)

SELECT * FROM final_fact
