{{ config(
    materialized = 'incremental',
    unique_key = ['shipment_id'],
    incremental_strategy = 'merge'
) }}

WITH shipment AS (
    SELECT
        shipment_id,
        product_id,
        customer_id,
        quantity_shipped,
        ship_date,
        destination
    FROM {{ ref('stg_shipment') }}
    {% if is_incremental() %}
      WHERE ship_date >= (SELECT MAX(ship_date) FROM {{ this }})
    {% endif %}
),

product AS (
    SELECT
        product_id,
        product_name,
        category,
        subcategory
    FROM {{ ref('stg_product') }}
),

production AS (
    SELECT
        product_id,
        MAX(end_date) AS latest_end_date,
        SUM(planned_quantity) AS total_planned_quantity
    FROM {{ ref('stg_production_order') }}
    GROUP BY 1
),

joined AS (
    SELECT
        s.shipment_id,
        s.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        s.customer_id,
        s.quantity_shipped,
        s.ship_date,
        pr.latest_end_date,
        pr.total_planned_quantity,
        s.destination
    FROM shipment s
    LEFT JOIN product p ON s.product_id = p.product_id
    LEFT JOIN production pr ON s.product_id = pr.product_id
),

metrics AS (
    SELECT
        *,
        DATEDIFF('day', latest_end_date, ship_date) AS delay_days,
        ROUND((quantity_shipped / NULLIF(total_planned_quantity, 0)) * 100, 2) AS fulfillment_rate_pct,
        CASE 
            WHEN ship_date <= latest_end_date THEN 'ON_TIME'
            WHEN DATEDIFF('day', latest_end_date, ship_date) BETWEEN 1 AND 3 THEN 'MINOR_DELAY'
            ELSE 'MAJOR_DELAY'
        END AS shipment_status
    FROM joined
)

SELECT
    shipment_id,
    product_id,
    product_name,
    category,
    subcategory,
    customer_id,
    quantity_shipped,
    total_planned_quantity,
    fulfillment_rate_pct,
    latest_end_date AS planned_completion_date,
    ship_date,
    delay_days,
    shipment_status,
    destination
FROM metrics
