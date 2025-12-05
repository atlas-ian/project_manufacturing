{{ config(
    materialized='table',
    unique_key='SUPPLIER_SK'
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
),

logic_layer AS (
    SELECT
        CAST(SUPPLIER_ID AS INT) AS supplier_id_int,
        TRIM(SUPPLIER_NAME) AS supplier_name_clean,
        TRIM(CONTACT_NAME) AS contact_name_clean,
        PHONE,
        
        -- Logic: Is the phone number valid length? (10-13 digits)
        LENGTH(REGEXP_REPLACE(SPLIT_PART(PHONE, 'x', 1), '[^0-9]', '')) AS _phone_digits_len,
        
        -- Logic: Does it have an extension?
        LENGTH(REGEXP_REPLACE(SPLIT_PART(PHONE, 'x', 2), '[^0-9]', '')) AS _ext_len

    FROM source_data
),

final_metrics AS (
    SELECT
        -- 1. Primary Key (Foreign Key to Dim Supplier)
        ABS(HASH(supplier_id_int)) AS SUPPLIER_SK,

        -- 2. Data Quality Metrics (The "Fact" part)
        -- Convert Boolean logic to 1 or 0 for aggregation
        
        -- Metric: Is the Phone Number usable?
        CASE 
            WHEN _phone_digits_len BETWEEN 10 AND 13 THEN 1 
            ELSE 0 
        END AS HAS_VALID_PHONE_FLG,

        -- Metric: Do we have a direct extension?
        CASE 
            WHEN _ext_len > 0 THEN 1 
            ELSE 0 
        END AS HAS_EXTENSION_FLG,

        -- Metric: Do we have a specific human contact?
        -- (Score 0 if Name is missing OR if Name is just the Company Name)
        CASE 
            WHEN contact_name_clean IS NULL THEN 0
            WHEN contact_name_clean = supplier_name_clean THEN 0
            ELSE 1 
        END AS HAS_HUMAN_CONTACT_FLG,

        -- 3. The "Wise" Aggregate Score
        -- A simple 0-100 score of how "rich" this profile is.
        -- (Phone + Contact) / 2 * 100
        CAST(
            (
             (CASE WHEN _phone_digits_len BETWEEN 10 AND 13 THEN 1 ELSE 0 END) +
             (CASE WHEN contact_name_clean != supplier_name_clean AND contact_name_clean IS NOT NULL THEN 1 ELSE 0 END)
            ) / 2.0 * 100 
        AS NUMBER) AS PROFILE_COMPLETENESS_SCORE

    FROM logic_layer
)

SELECT * FROM final_metrics