{{ config(
    materialized = 'incremental',
    unique_key = ['production_order_id'],
    incremental_strategy = 'merge'
) }}

WITH production AS (
    SELECT
        production_order_id,
        product_id,
        machine_id,
        shift_id,
        planned_quantity,
        start_date,
        end_date,
        DATEDIFF('hour', start_date, end_date) AS production_hours
    FROM {{ ref('stg_production_order') }}
    {% if is_incremental() %}
      WHERE end_date >= (SELECT MAX(end_date) FROM {{ this }})
    {% endif %}
),

machine AS (
    SELECT
        machine_id,
        department,
        machine_type
    FROM {{ ref('stg_machine') }}
),

joined AS (
    SELECT
        p.production_order_id,
        p.product_id,
        p.machine_id,
        m.department,
        m.machine_type,
        p.shift_id,
        p.planned_quantity,
        p.production_hours,
        p.start_date,
        p.end_date
    FROM production p
    LEFT JOIN machine m 
        ON p.machine_id = m.machine_id
),

metrics AS (
    SELECT
        *,
        ROUND(planned_quantity / NULLIF(production_hours, 0), 2) AS throughput_units_per_hour,
        ROUND(
            (CASE 
                WHEN production_hours > 0 THEN 
                    planned_quantity / production_hours 
                ELSE NULL 
             END) / 10 * 100, 2
        ) AS efficiency_score_pct,
        CASE 
            WHEN planned_quantity / NULLIF(production_hours, 0) < 5 THEN 'LOW_EFFICIENCY'
            WHEN planned_quantity / NULLIF(production_hours, 0) BETWEEN 5 AND 15 THEN 'NORMAL_EFFICIENCY'
            ELSE 'HIGH_EFFICIENCY'
        END AS efficiency_status
    FROM joined
)

SELECT
    production_order_id,
    product_id,
    machine_id,
    department,
    machine_type,
    shift_id,
    planned_quantity,
    production_hours,
    throughput_units_per_hour,
    efficiency_score_pct,
    efficiency_status,
    start_date,
    end_date
FROM metrics
