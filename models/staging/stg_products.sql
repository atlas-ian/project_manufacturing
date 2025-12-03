WITH products AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME ,
        SUPPLIER_ID,
        CATEGORY,
        SUBCATEGORY,
        UNIT_OF_MEASURE,
        CREATED_TS
    FROM {{ source('src', 'raw_product') }}
)

SELECT * FROM products
