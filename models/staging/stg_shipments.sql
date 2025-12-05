WITH shippments AS (
    SELECT
        shipment_id,
        product_id,
        warehouse_id,
       quantity,
        shipment_date,
        delivery_date,
        status
       
    FROM {{ source('src', 'raw_shipment') }}

)
SELECT * 
FROM shippments
