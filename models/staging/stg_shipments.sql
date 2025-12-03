{{ config(
    materialized = 'incremental',
    unique_key = 'shipment_id',
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (

    SELECT
        shipment_id,
        customer_id,
        product_id,
        quantity_shipped,
        destination,
        ship_date,
        CURRENT_TIMESTAMP() AS record_loaded_ts
    FROM {{ source('src', 'raw_shipment') }}

    {% if is_incremental() %}
        -- Only fetch new or updated shipments
        WHERE ship_date > (
            SELECT COALESCE(MAX(ship_date), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
),

renamed AS (
    SELECT
        TRIM(shipment_id)                    AS shipment_id,
        TRIM(customer_id)                    AS customer_id,
        TRIM(product_id)                     AS product_id,
        quantity_shipped                     AS quantity_shipped,
        INITCAP(TRIM(destination))           AS destination,
        ship_date,
        record_loaded_ts
    FROM source
)

SELECT * 
FROM renamed
