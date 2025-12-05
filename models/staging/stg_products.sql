WITH products AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME ,
        SUPPLIER_ID,
        CATEGORY,
        UNIT_OF_MEASURE,
        WEIGHT_KG,
        PRICE,
        CREATED_TS
    FROM {{ source('src', 'raw_product') }}
)

SELECT * FROM products
