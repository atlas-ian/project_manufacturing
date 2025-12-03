{{ config(
    materialized = 'incremental',
    unique_key = ['machine_id', 'production_date'],
    incremental_strategy = 'merge'
) }}

WITH machine_data AS (
    SELECT
        machine_id,
        machine_type,
        department,
        install_date,
        status
    FROM {{ ref('stg_machine') }}
),

production_data AS (
    SELECT
        machine_id,
        production_order_id,
        start_date,
        end_date,
        shift_id,
        planned_quantity,
        DATEDIFF('hour', start_date, end_date) AS production_hours
    FROM {{ ref('stg_production_order') }}
    {% if is_incremental() %}
      WHERE end_date >= (SELECT MAX(production_date) FROM {{ this }})
    {% endif %}
),

-- Aggregate machine usage by day
machine_usage AS (
    SELECT
        machine_id,
        DATE_TRUNC('day', start_date) AS production_date,
        COUNT(DISTINCT production_order_id) AS total_orders,
        SUM(production_hours) AS total_production_hours,
        SUM(planned_quantity) AS total_units_planned
    FROM production_data
    GROUP BY 1,2
),

-- Assume available machine time = 24 hours/day
metrics AS (
    SELECT
        mu.machine_id,
        mu.production_date,
        md.machine_type,
        md.department,
        md.status,
        mu.total_orders,
        mu.total_production_hours,
        mu.total_units_planned,
        24 AS available_hours,
        ROUND((mu.total_production_hours / 24) * 100, 2) AS utilization_rate_pct,

        -- Derived performance metrics
        ROUND(mu.total_units_planned / NULLIF(mu.total_production_hours,0), 2) AS throughput_units_per_hour,
        ROUND((24 - mu.total_production_hours), 2) AS idle_hours,
        CASE 
            WHEN (mu.total_production_hours / 24) * 100 < 60 THEN 'UNDERUTILIZED'
            WHEN (mu.total_production_hours / 24) * 100 BETWEEN 60 AND 90 THEN 'OPTIMAL'
            ELSE 'OVERLOADED'
        END AS utilization_status
    FROM machine_usage mu
    LEFT JOIN machine_data md ON mu.machine_id = md.machine_id
)

SELECT
    machine_id,
    production_date,
    department,
    machine_type,
    status AS machine_status,
    total_orders,
    total_units_planned,
    total_production_hours,
    available_hours,
    idle_hours,
    utilization_rate_pct,
    throughput_units_per_hour,
    utilization_status
FROM metrics