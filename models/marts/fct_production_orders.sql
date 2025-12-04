{{ config(
    materialized='table',
    unique_key='production_order_sk'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('int_production_metrics') }}

),

final_fact AS (

    SELECT
        -- Surrogate Key
        ABS(HASH(production_order_id || TO_CHAR(start_date, 'YYYYMMDD'))) AS production_order_sk,

        -- Foreign Keys
        production_order_id,
        product_id,
        machine_id,

        -- Descriptive Attributes
        machine_type,
        department,
        order_status,
        machine_current_status,

        -- Measures
        planned_quantity,
        production_hours,
        throughput_units_per_hour,
        efficiency_score_pct,

        -- Flags / Categories
        efficiency_status,

        -- Original Dates
        start_date,
        end_date

    FROM source_data
)

SELECT * FROM final_fact
