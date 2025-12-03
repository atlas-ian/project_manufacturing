{{ config(
    materialized = 'view'
) }}

WITH machines AS (
    SELECT
        MACHINE_ID          AS machine_id,
        MACHINE_TYPE        AS machine_type,
        CAPACITY_PER_DAY    AS capacity_per_day,
        INSTALL_DATE        AS install_date,
        STATUS              AS status
    FROM {{ source('src', 'raw_machine') }}
)

SELECT * FROM machines
