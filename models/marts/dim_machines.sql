{{ config(
    materialized='table',
    unique_key='MACHINE_SK'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('src', 'raw_machine') }}
),

logic_layer AS (
    SELECT
        CAST(MACHINE_ID AS INT) AS machine_id_int,
        TRIM(MACHINE_TYPE) AS machine_type,
        CAST(INSTALL_DATE AS DATE) AS install_date,
        TRIM(STATUS) AS status,
        CAST(CAPACITY_PER_DAY AS INT) AS capacity,

        -- Derived Department
        CASE 
            WHEN TRIM(MACHINE_TYPE) IN ('Drill', 'Milling', 'Lathe', 'CNC') THEN 'Machining'
            WHEN TRIM(MACHINE_TYPE) IN ('3D Printer', 'Laser Cutter') THEN 'Fabrication'
            ELSE 'General Assembly'
        END AS department

    FROM source_data
),


final_enrichment AS (
    SELECT
        -- Keys
        ABS(HASH(machine_id_int)) AS MACHINE_SK,
        CAST(machine_id_int AS VARCHAR) AS MACHINE_ID,
        
        -- Core Info
        machine_type AS MACHINE_TYPE,
        department AS DEPARTMENT,
        install_date AS INSTALL_DATE,
        status AS STATUS,

        -- 1. CAPACITY TIER
        CASE 
            WHEN capacity >= 400 THEN 'Heavy Duty'
            WHEN capacity >= 200 THEN 'Standard'
            ELSE 'Light Duty'
        END AS CAPACITY_TIER,

        -- 2. WARRANTY CHECK (FIXED)
        -- Changed from 730 days (2 years) to 365 days (1 year).
        -- Why? Because your data is recent. This forces a split between New vs Old machines.
        CASE 
            WHEN DATEDIFF('day', install_date, CURRENT_DATE()) <= 365 THEN TRUE 
            ELSE FALSE 
        END AS IS_UNDER_WARRANTY,

        -- 3. OPERATIONAL FLAG
        CASE 
            WHEN status = 'Active' THEN TRUE 
            ELSE FALSE 
        END AS IS_OPERATIONAL

    FROM logic_layer
)

SELECT * FROM final_enrichment



--"If the machine was installed less than 365 days ago, the Warranty is TRUE. Otherwise, it is FALSE."