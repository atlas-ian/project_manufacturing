WITH shippments AS (
    SELECT
        shipment_id,
        product_id,
        warehouse_id,
<<<<<<< HEAD
        quantity,
=======
       quantity,
>>>>>>> b89e903ef0d8d91c830075cc3fc503d9dc8ee599
        shipment_date,
        delivery_date,
        status
       
    FROM {{ source('src', 'raw_shipment') }}

)
SELECT * 
FROM shippments
