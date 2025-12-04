{{ config(
    materialized='table',
    unique_key='SUPPLIER_SK'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('src', 'raw_supplier') }}
),

stage_formatting AS (
    SELECT
        CAST(SUPPLIER_ID AS INT) AS supplier_id_int,
        TRIM(SUPPLIER_NAME) AS supplier_name_clean,
        INITCAP(TRIM(COUNTRY)) AS country_clean,
        TRIM(CONTACT_NAME) AS contact_name_clean,
        
        -- PRE-PROCESSING:
        -- 1. Split at 'x'
        -- 2. Clean digits
        REGEXP_REPLACE(SPLIT_PART(PHONE, 'x', 1), '[^0-9]', '') AS _digits_main,
        REGEXP_REPLACE(SPLIT_PART(PHONE, 'x', 2), '[^0-9]', '') AS _digits_ext
    FROM source_data
),

final_polish AS (
    SELECT
        -- 1. Keys
        ABS(HASH(supplier_id_int)) AS SUPPLIER_SK,
        CAST(supplier_id_int AS VARCHAR) AS SUPPLIER_ID,
        
        -- 2. Core Info
        supplier_name_clean AS SUPPLIER_NAME,
        country_clean AS COUNTRY,
        
        -- 3. Contact Logic
        CASE 
            WHEN contact_name_clean = supplier_name_clean THEN 'General Inquiry'
            WHEN contact_name_clean IS NULL THEN 'Unknown'
            ELSE contact_name_clean 
        END AS CONTACT_PERSON,

        -- 4. THE ROBUST PHONE FORMATTER
        CASE 
            -- Case A: Standard 10-Digit (US/Can) -> (909) 414-9851
            WHEN LENGTH(_digits_main) = 10 THEN 
                '(' || SUBSTR(_digits_main, 1, 3) || ') ' || 
                SUBSTR(_digits_main, 4, 3) || '-' || 
                SUBSTR(_digits_main, 7, 4)

            -- Case B: 11-Digit with Country Code 1 -> +1 (814) 547-4430
            WHEN LENGTH(_digits_main) = 11 AND STARTSWITH(_digits_main, '1') THEN 
                '+1 (' || SUBSTR(_digits_main, 2, 3) || ') ' || 
                SUBSTR(_digits_main, 5, 3) || '-' || 
                SUBSTR(_digits_main, 8, 4)

            -- Case C (THE FIX): 13-Digit International (001...) -> 001 (764) 972-2981
            -- This catches the specific error you found
            WHEN LENGTH(_digits_main) = 13 AND STARTSWITH(_digits_main, '001') THEN 
                '001 (' || SUBSTR(_digits_main, 4, 3) || ') ' || 
                SUBSTR(_digits_main, 7, 3) || '-' || 
                SUBSTR(_digits_main, 10, 4)

            -- Case D: Fallback for any other long numbers
            -- If it's 12+ digits but not 001, we just space it out for readability:
            -- 123456789012 -> 123-456-789-012
            WHEN LENGTH(_digits_main) > 10 THEN
                 REGEXP_REPLACE(_digits_main, '(\\d{3})', '\\1-') 
            
            ELSE _digits_main
        END 
        -- Append Extension
        || CASE 
            WHEN LEN(_digits_ext) > 0 THEN ' x' || _digits_ext
            ELSE '' 
           END 
        AS PHONE_NUMBER

    FROM stage_formatting
)

SELECT * FROM final_polish