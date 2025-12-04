{{ config(
    materialized='table',
    unique_key='product_key'
) }}

WITH source_data AS (
    -- Pull distinct product-level fields from intermediate model (NO raw refs)
    SELECT DISTINCT
        product_name,
        category,
        product_id
    FROM {{ ref('int_shipment_metrics') }}
),

parsed_logic AS (
    SELECT
        product_name AS original_label,
        category AS category_name,

        -- Extract text product name (everything before last number group)
        TRIM(REGEXP_SUBSTR(product_name, '^[a-zA-Z ]+')) AS product_name_clean,

        -- Extract product id number from product_name OR default to product_id
        COALESCE(
            CAST(REGEXP_SUBSTR(product_name, '[0-9]+$') AS INT),
            CAST(product_id AS INT)
        ) AS source_product_id

    FROM source_data
),

sku_generation AS (
    SELECT
        *,
        CASE category_name
            WHEN 'Appliances'   THEN 'APP'
            WHEN 'Textiles'     THEN 'TEX'
            WHEN 'Furniture'    THEN 'FUR'
            WHEN 'Electronics'  THEN 'ELE'
            WHEN 'Automobile'   THEN 'AUT'
            WHEN 'Toys'         THEN 'TOY'
            ELSE 'UNK'
        END AS category_code
    FROM parsed_logic
),

final_dimension AS (
    SELECT
        -- Surrogate Key
        MD5(original_label || category_name) AS product_key,

        -- Human readable SKU
        category_code || '-' || LPAD(CAST(source_product_id AS VARCHAR), 4, '0') AS product_sku,

        -- Clean attributes
        product_name_clean AS product_name,
        category_name AS product_category,
        source_product_id,
        original_label AS product_label_search,

        -- New meaningful columns
        LENGTH(original_label) AS product_label_length,          -- text length for QA or analysis
        CASE WHEN category_name = 'Electronics' THEN TRUE ELSE FALSE END AS is_electronic  -- useful boolean flag

    FROM sku_generation
)

SELECT * FROM final_dimension