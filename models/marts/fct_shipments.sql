{{ config(
    materialized='table',
    unique_key='shipment_sk'
) }}

WITH source_data AS (

    SELECT *
    FROM {{ ref('int_shipment_metrics') }}

),

final_fact AS (

    SELECT
        -- Surrogate Key
        ABS(HASH(shipment_id || TO_CHAR(ship_date, 'YYYYMMDD'))) AS shipment_sk,

        -- Foreign Keys
        shipment_id,
        product_id,

        -- Descriptive Attributes
        product_name,
        category,
        shipment_status,

        -- Measures
        quantity_shipped,
        total_planned_quantity,
        fulfillment_rate_pct,
        transit_days,
        delay_days,

        -- Categories / Flags
        shipment_timing_status,

        -- Reference Dates
        ship_date,
        delivery_date,
        planned_completion_date

    FROM source_data
)

SELECT * FROM final_fact
