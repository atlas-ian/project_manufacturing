{{ config(
    materialized = 'incremental',
    unique_key = 'machine_id_production_date'
) }}

<<<<<<< HEAD
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
=======
with machines as (

    select
        -- Best Practice: Clean join keys to avoid silent failures
        upper(trim(machine_id)) as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as current_machine_status
    from {{ source('src', 'raw_machine') }}

),

orders as (

    select
        upper(trim(machine_id)) as join_key,
        machine_id as raw_order_machine_id,
        
        -- DDL defines this as DATE, so we use it directly
        start_date as production_date,
        
        count(production_order_id) as total_orders,
        
        -- Matching your DDL column name
        sum(planned_quantity) as total_units_planned

    from {{ source('src', 'raw_production_order') }}
    group by 1, 2, 3

),

joined_data as (

    select
        orders.production_date,
        -- If machine table is missing the ID, preserve the ID from the order
        coalesce(machines.machine_id, orders.raw_order_machine_id) as machine_id,
        machines.machine_type,
        machines.current_machine_status as machine_status,
        orders.total_orders,
        orders.total_units_planned,
        machines.capacity_per_day,
        
        -- Department Mapping
        case 
            when machines.machine_type in ('Drill', 'Lathe', 'Milling') then 'Standard Machining'
            when machines.machine_type in ('CNC') then 'Advanced Machining'
            when machines.machine_type = 'Laser Cutter' then 'Fabrication'
            when machines.machine_type = '3D Printer' then 'Additive Manufacturing'
            else 'Other'
        end as department

    from orders
    left join machines 
        on orders.join_key = machines.join_key

),

metrics_calculation as (

    select
        *,
        24 as available_hours,

        -- Calculate Production Hours
        -- Formula: (Total Units / Daily Capacity) * 24
        round(
            (total_units_planned / nullif(capacity_per_day, 0)) * 24, 
        2) as total_production_hours

    from joined_data

),

final as (

    select
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
        coalesce(total_orders, 0) as total_orders,
        coalesce(total_units_planned, 0) as total_units_planned,
        
        -- Time Metrics
        total_production_hours,
        available_hours,
        round(greatest(0, available_hours - total_production_hours), 2) as idle_hours,

        -- Utilization Rate %
        round(
            (total_units_planned / nullif(capacity_per_day, 0)) * 100, 
        2) as utilization_rate_pct,

        -- Throughput
        round(
            total_units_planned / nullif(total_production_hours, 0), 
        2) as throughput_units_per_hour,

        -- Status Logic
        case
            when capacity_per_day is null then 'Unknown Capacity'
            when (total_units_planned / nullif(capacity_per_day, 0)) > 1.0 then 'Overloaded'
            when (total_units_planned / nullif(capacity_per_day, 0)) >= 0.8 then 'Optimal'
            when (total_units_planned / nullif(capacity_per_day, 0)) >= 0.5 then 'Underutilized'
            else 'Idle/Low'
        end as utilization_status

    from metrics_calculation

)

select * from final

>>>>>>> eb53dc1215facab3ad23f1ba7fd942e96817cfc0
