WITH products AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME ,
        CATEGORY,
        SUBCATEGORY,
        UNIT_OF_MEASURE,
        CREATED_TS
    FROM {{ source('src', 'raw_product') }}
)

SELECT * FROM products
