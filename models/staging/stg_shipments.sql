WITH shippments AS (
    SELECT
        shipment_id,
        customer_id,
        product_id,
        quantity_shipped,
        destination,
        ship_date
    FROM {{ source('src', 'raw_shipment') }}

)
SELECT * 
FROM shippments
