{{ config(
    materialized = 'incremental',
    unique_key = 'machine_id_production_date'
) }}

-- ============================
-- MACHINE RAW / STAGING DATA
-- ============================
WITH machine AS (
    SELECT
        machine_id,
        machine_type,
        status AS machine_status,
        capacity_per_day,
        install_date
    FROM {{ ref('stg_machines') }}
),

-- ============================
-- PRODUCTION ORDERS
-- ============================
prod AS (
    SELECT
        production_order_id,
        machine_id,
        planned_quantity,
        start_date,
        end_date,
        DATE(start_date) AS production_date,
        TIMESTAMPDIFF(hour, start_date, end_date) AS production_hours
    FROM {{ ref('stg_production_orders') }}

    {% if is_incremental() %}
        WHERE start_date > (
            SELECT COALESCE(MAX(production_date), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
),

-- ============================
-- AGGREGATIONS PER MACHINE-DATE
-- ============================
aggregated AS (
    SELECT
        machine_id,
        production_date,
        COUNT(*) AS total_orders,
        SUM(planned_quantity) AS total_units_planned,
        SUM(production_hours) AS total_production_hours
    FROM prod
    GROUP BY machine_id, production_date
),

-- ============================
-- FINAL UTILIZATION METRICS
-- ============================
final AS (
    SELECT
        m.machine_id,
        COALESCE(a.production_date, CURRENT_DATE()) AS production_date,
        m.machine_type,
        m.machine_status,

        -- Totals
        COALESCE(a.total_orders, 0) AS total_orders,
        COALESCE(a.total_units_planned, 0) AS total_units_planned,
        COALESCE(a.total_production_hours, 0) AS total_production_hours,

        -- Capacity from machine table
        m.capacity_per_day AS available_hours,

        -- Idle = available - actual used
        (m.capacity_per_day - COALESCE(a.total_production_hours, 0)) AS idle_hours,

        -- Utilization %
        CASE 
            WHEN m.capacity_per_day > 0 
                THEN ROUND(
                        (COALESCE(a.total_production_hours, 0) 
                        / m.capacity_per_day) * 100, 2
                    )
            ELSE 0 
        END AS utilization_rate_pct,

        -- Throughput units per hour
        CASE 
            WHEN COALESCE(a.total_production_hours, 0) > 0
                THEN ROUND(
                        a.total_units_planned 
                        / a.total_production_hours, 2
                    )
            ELSE 0 
        END AS throughput_units_per_hour,

        -- Utilization Category
        CASE 
            WHEN m.machine_status != 'active' 
                THEN 'not operational'
            WHEN m.capacity_per_day = 0 
                THEN 'invalid capacity'
            WHEN (COALESCE(a.total_production_hours, 0) 
                 / m.capacity_per_day) >= 0.85 
                THEN 'over utilized'
            WHEN (COALESCE(a.total_production_hours, 0) 
                 / m.capacity_per_day) BETWEEN 0.60 AND 0.85 
                THEN 'optimal'
            WHEN (COALESCE(a.total_production_hours, 0) 
                 / m.capacity_per_day) BETWEEN 0.30 AND 0.60 
                THEN 'under utilized'
            ELSE 'idle'
        END AS utilization_status

    FROM machine m
    LEFT JOIN aggregated a 
        ON m.machine_id = a.machine_id
)

SELECT * FROM final
