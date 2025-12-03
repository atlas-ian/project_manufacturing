SELECT
    PRODUCTION_ORDER_ID AS production_order_id,
    PRODUCT_ID          AS product_id,
    PLANNED_QUANTITY    AS planned_quantity,
    START_DATE          AS start_date,
    END_DATE            AS end_date,
    MACHINE_ID          AS machine_id,
    SHIFT_ID            AS shift_id
FROM {{ source('src', 'raw_production_order') }}