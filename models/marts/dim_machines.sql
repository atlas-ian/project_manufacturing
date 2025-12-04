-- models/marts/dim_machines.sql

{{ config(
    materialized='table',
    unique_key='MACHINE_SK'
) }}

WITH source_data AS (
    SELECT
        machine_id,
        machine_type,
        department,
        install_date,
        machine_status AS status,
        capacity_per_day  
    FROM {{ ref('int_machine_utilization') }}
),

logic_layer AS (
    SELECT
        CAST(machine_id AS INT) AS machine_id_int,
        TRIM(machine_type) AS machine_type,
        CAST(install_date AS DATE) AS install_date,
        TRIM(status) AS status,
        CAST(capacity_per_day AS INT) AS capacity,
        department,

        -- Machine Age
        DATEDIFF('day', install_date, CURRENT_DATE()) AS machine_age_days

    FROM source_data
),

final_enrichment AS (
    SELECT
        -- Surrogate Key
        ABS(HASH(machine_id_int)) AS MACHINE_SK,
        CAST(machine_id_int AS VARCHAR) AS MACHINE_ID,

        -- Core Info
        machine_type AS MACHINE_TYPE,
        department AS DEPARTMENT,
        install_date AS INSTALL_DATE,
        status AS STATUS,

        -- 1. Capacity Tier
        CASE 
            WHEN capacity >= 400 THEN 'Heavy Duty'
            WHEN capacity >= 200 THEN 'Standard'
            ELSE 'Light Duty'
        END AS CAPACITY_TIER,

        -- 2. Warranty Check
        CASE 
            WHEN DATEDIFF('day', install_date, CURRENT_DATE()) <= 365 THEN TRUE
            ELSE FALSE
        END AS IS_UNDER_WARRANTY,

        -- 3. Operational Flag
        CASE 
            WHEN status = 'Active' THEN TRUE
            ELSE FALSE
        END AS IS_OPERATIONAL,

        -- 4. Machine Age in Days
        machine_age_days,

        -- 5. Machine Age Category
        CASE
            WHEN machine_age_days <= 365 THEN 'New'
            WHEN machine_age_days <= 3 * 365 THEN '1-3 Years'
            WHEN machine_age_days <= 5 * 365 THEN '3-5 Years'
            ELSE 'Old'
        END AS MACHINE_AGE_CATEGORY,

        -- 6. Performance Class
        CASE
            WHEN capacity >= 400 AND machine_age_days < 3 * 365 THEN 'High Performance'
            WHEN capacity >= 200 THEN 'Standard Performance'
            ELSE 'Low Performance'
        END AS PERFORMANCE_CLASS,

        -- 7. Maintenance Risk Level
        CASE
            WHEN status <> 'Active' THEN 'High'
            WHEN machine_age_days > 5 * 365 THEN 'High'
            WHEN machine_age_days > 3 * 365 THEN 'Medium'
            ELSE 'Low'
        END AS MAINTENANCE_RISK_LEVEL

    FROM logic_layer
)

SELECT * FROM final_enrichment
