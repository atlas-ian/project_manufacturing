WITH machines AS (
    SELECT
        upper(trim(MACHINE_ID)) AS join_key,
        MACHINE_ID AS machine_id,
        MACHINE_TYPE AS machine_type,
        CAPACITY_PER_DAY AS capacity_per_day,  -- ensure correct name
        INSTALL_DATE AS install_date,
        STATUS AS current_machine_status
    FROM {{ source('src', 'raw_machine') }}
),

orders AS (
    SELECT
        upper(trim(machine_id)) AS join_key,
        machine_id AS raw_order_machine_id,
        start_date AS production_date,
        count(production_order_id) AS total_orders,
        sum(planned_quantity) AS total_units_planned
    FROM {{ source('src', 'raw_production_order') }}
    GROUP BY 1,2,3
),

joined_data AS (
    SELECT
        orders.production_date,
        coalesce(machines.machine_id, orders.raw_order_machine_id) AS machine_id,
        machines.machine_type,
        machines.install_date,
        machines.current_machine_status AS machine_status,
        orders.total_orders,
        orders.total_units_planned,
        machines.capacity_per_day,  -- propagate column

        -- Department Mapping
        CASE 
            WHEN machines.machine_type IN ('Drill', 'Lathe', 'Milling') THEN 'Standard Machining'
            WHEN machines.machine_type = 'CNC' THEN 'Advanced Machining'
            WHEN machines.machine_type = 'Laser Cutter' THEN 'Fabrication'
            WHEN machines.machine_type = '3D Printer' THEN 'Additive Manufacturing'
            ELSE 'Other'
        END AS department

    FROM orders
    LEFT JOIN machines 
        ON orders.join_key = machines.join_key
),

metrics_calculation AS (
    SELECT
        *,
        24 AS available_hours,
        ROUND((total_units_planned / NULLIF(capacity_per_day, 0)) * 24, 2) AS total_production_hours
    FROM joined_data
),

final AS (
    SELECT
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
        install_date,
        coalesce(total_orders, 0) AS total_orders,
        coalesce(total_units_planned, 0) AS total_units_planned,
        capacity_per_day,  -- include for downstream dim table
        total_production_hours,
        available_hours,
        ROUND(GREATEST(0, available_hours - total_production_hours), 2) AS idle_hours,
        ROUND((total_units_planned / NULLIF(capacity_per_day, 0)) * 100, 2) AS utilization_rate_pct,
        ROUND(total_units_planned / NULLIF(total_production_hours, 0), 2) AS throughput_units_per_hour,
        CASE
            WHEN capacity_per_day IS NULL THEN 'Unknown Capacity'
            WHEN (total_units_planned / NULLIF(capacity_per_day, 0)) > 1.0 THEN 'Overloaded'
            WHEN (total_units_planned / NULLIF(capacity_per_day, 0)) >= 0.8 THEN 'Optimal'
            WHEN (total_units_planned / NULLIF(capacity_per_day, 0)) >= 0.5 THEN 'Underutilized'
            ELSE 'Idle/Low'
        END AS utilization_status,
        CASE 
            WHEN install_date IS NULL THEN 'Unknown'
            WHEN DATEDIFF('day', install_date, CURRENT_DATE()) <= 365 THEN 'Commissioned'
            WHEN DATEDIFF('day', install_date, CURRENT_DATE()) <= 3 * 365 THEN 'Mid-Life'
            WHEN DATEDIFF('day', install_date, CURRENT_DATE()) <= 6 * 365 THEN 'Aging'
            ELSE 'End-of-Life'
        END AS lifecycle_stage
    FROM metrics_calculation
)

SELECT * FROM final
