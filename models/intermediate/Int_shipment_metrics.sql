-- int_shipment_metrics.sql
-- Intermediate model built from staging source models:
--   stg_shipment
--   stg_product
--   stg_production_order

WITH shipments AS (
    SELECT
        shipment_id,
        product_id,
        quantity            AS quantity_shipped,
        delivery_date,
        shipment_status,
        destination
    from {{ source('src', 'raw_shipment') }}
),

products AS (
    SELECT
        product_id,
        product_name,
        category,
    from {{ source('src', 'raw_product') }}
),

production_orders AS (
    SELECT
        product_id,
        planned_quantity,
        end_date
    from {{ source('src', 'raw_production_order') }}
),

production_agg AS (
    SELECT
        product_id,
        SUM(planned_quantity) AS total_planned_quantity,
        MAX(end_date)         AS latest_end_date
    FROM production_orders
    GROUP BY product_id
)

SELECT
    s.shipment_id,
    s.product_id,
    p.product_name,
    p.category,
    s.quantity_shipped,
    COALESCE(pa.total_planned_quantity, 0) AS total_planned_quantity,

    CASE WHEN COALESCE(pa.total_planned_quantity, 0) = 0 THEN NULL
         ELSE ROUND((s.quantity_shipped / pa.total_planned_quantity) * 100, 2)
    END AS fulfillment_rate_pct,

    pa.latest_end_date AS planned_completion_date,
   

    DATEDIFF('day',  s.delivery_date) AS delay_days,

    s.shipment_status,
    s.destination

FROM shipments s
LEFT JOIN products p
    ON s.product_id = p.product_id
LEFT JOIN production_agg pa
    ON s.product_id = pa.product_id
