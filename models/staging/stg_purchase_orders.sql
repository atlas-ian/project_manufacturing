-- models/staging/stg_machine.sql
SELECT
    PO_ID as PO_ID,
    SUPPLIER_ID as supplier_id ,
    PRODUCT_ID as product_id ,
    QUANTITY as quantity,
    UNIT_PRICE as unit_price,
    ORDER_DATE as order_date,
    EXPECTED_DELIVERY as expected_delivery
FROM {{ source('src', 'raw_purchase_order') }}
