{{ config(
    materialized='table',
    unique_key='product_key'
) }}

WITH source_data AS (
    -- Reference your raw data or staging model here
    SELECT 
        PRODUCT_NAME, 
        CATEGORY
    from {{ source('src', 'raw_product') }}
),

parsed_logic AS (
    SELECT
        PRODUCT_NAME AS original_label,
        CATEGORY AS category_name,
        
        -- 1. Extract the Text Name (Everything before the last space)
        -- Logic: Trims whitespace, assumes format "Name ID"
        TRIM(REGEXP_SUBSTR(PRODUCT_NAME, '^[a-zA-Z ]+')) AS product_name_clean,

        -- 2. Extract the Source ID (The numbers at the end)
        -- Logic: Casts the extracted regex number to Integer
        CAST(REGEXP_SUBSTR(PRODUCT_NAME, '[0-9]+$') AS INT) AS source_product_id
    FROM source_data
),

sku_generation AS (
    SELECT
        *,
        -- 3. Create a short code for the Category
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
        -- 4. Create the Surrogate Key (Primary Key)
        -- Using MD5 hash ensures the same ID is generated every run for the same product
        MD5(original_label || category_name) AS product_key,

        -- 5. Create the Human-Readable SKU (e.g., TEX-0022)
        -- LPAD ensures '22' becomes '0022' for sorting alignment
        category_code || '-' || LPAD(CAST(source_product_id AS VARCHAR), 4, '0') AS product_sku,

        -- Clean Columns
        product_name_clean AS product_name,
        category_name AS product_category,
        source_product_id,
        original_label AS product_label_search

    FROM sku_generation
)

SELECT * FROM final_dimension