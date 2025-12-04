
with prod as (
SELECT
    PRODUCTION_ORDER_ID     AS production_order_id,
    PRODUCT_ID  as product_id,
    MACHINE_ID               AS machine_id,
    PLANNED_QUANTITY        AS planned_quantity,
    START_DATE              AS start_date,
    END_DATE                AS end_date,
               
    STATUS               AS status
from {{ source ('src', 'raw_production_order')}}
)
SELECT * from prod
