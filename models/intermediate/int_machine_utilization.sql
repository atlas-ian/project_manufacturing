<<<<<<< HEAD
<<<<<<< HEAD
{{ config(materialized='table') }}

with machines as (

    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as current_machine_status
    from {{ ref('stg_machines') }}

),

orders as (

    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id as raw_order_machine_id,
        start_date as production_date,
        count(production_order_id) as total_orders,
        sum(planned_quantity) as total_units_planned
    from {{ ref('stg_production_orders') }}
    group by 1,2,3

),

joined_data as (

    select
        o.production_date,
        coalesce(m.machine_id, o.raw_order_machine_id) as machine_id,
        m.machine_type,
        m.current_machine_status as machine_status,
        o.total_orders,
        o.total_units_planned,
        m.capacity_per_day,
        {{ map_machine_department('m.machine_type') }} as department
    from orders o
    left join machines m
        on o.join_key = m.join_key
=======
WITH machines AS (
    SELECT
        upper(trim(MACHINE_ID)) AS join_key,
        MACHINE_ID AS machine_id,
        MACHINE_TYPE AS machine_type,
        CAPACITY_PER_DAY AS capacity_per_day,  -- ensure correct name
        INSTALL_DATE AS install_date,
        STATUS AS current_machine_status
    FROM {{ source('src', 'raw_machine') }}
=======
{{ config(
    materialized='table'
) }}

with machines as (
    select
        {{ clean_id('MACHINE_ID') }} as join_key,
        MACHINE_ID as machine_id,
        MACHINE_TYPE as machine_type,
        CAPACITY_PER_DAY as capacity_per_day,
        INSTALL_DATE as install_date,
        STATUS as current_machine_status
    from {{ source('src', 'raw_machine') }}
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
),

orders as (
    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id as raw_order_machine_id,
        start_date as production_date,
        count(production_order_id) as total_orders,
        sum(planned_quantity) as total_units_planned
    from {{ source('src', 'raw_production_order') }}
    group by 1,2,3
),

joined_data as (
    select
        orders.production_date,
        coalesce(machines.machine_id, orders.raw_order_machine_id) as machine_id,
        machines.machine_type,
        machines.install_date,
        machines.current_machine_status as machine_status,
        orders.total_orders,
        orders.total_units_planned,
<<<<<<< HEAD
        machines.capacity_per_day,  -- propagate column

        -- Department Mapping
        CASE 
            WHEN machines.machine_type IN ('Drill', 'Lathe', 'Milling') THEN 'Standard Machining'
            WHEN machines.machine_type = 'CNC' THEN 'Advanced Machining'
            WHEN machines.machine_type = 'Laser Cutter' THEN 'Fabrication'
            WHEN machines.machine_type = '3D Printer' THEN 'Additive Manufacturing'
            ELSE 'Other'
        END AS department
>>>>>>> 5a283c115a78f476192decb4c80fce057f5a28cc

    FROM orders
    LEFT JOIN machines 
        ON orders.join_key = machines.join_key
),

<<<<<<< HEAD
metrics as (

    select
        *,
        24 as available_hours,

        -- KEY FIX: do the * 24 inside SQL, not in Jinja
        round(
            {{ utilization_ratio('total_units_planned', 'capacity_per_day') }} * 24,
            2
        ) as total_production_hours

    from joined_data

=======
metrics_calculation AS (
    SELECT
        *,
        24 AS available_hours,
        ROUND((total_units_planned / NULLIF(capacity_per_day, 0)) * 24, 2) AS total_production_hours
    FROM joined_data
>>>>>>> 5a283c115a78f476192decb4c80fce057f5a28cc
=======
        machines.capacity_per_day,
        -- Department mapping using macro
        {{ map_machine_department('machines.machine_type') }} as department
    from orders
    left join machines
        on orders.join_key = machines.join_key
),

metrics_calculation as (
    select
        *,
        24 as available_hours,
        {{ production_hours('production_date', 'production_date') }} as total_production_hours  -- We'll override to 24 hours per day
    from joined_data
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
),

final as (
    select
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
<<<<<<< HEAD
        coalesce(total_orders, 0) as total_orders,
        coalesce(total_units_planned, 0) as total_units_planned,

        total_production_hours,
        available_hours,

        {{ safe_round(
            'greatest(0, available_hours - total_production_hours)',
            2
        ) }} as idle_hours,

        {{ pct('total_units_planned', 'capacity_per_day') }} as utilization_rate_pct,

        {{ throughput_units_per_hour('total_units_planned', 'total_production_hours') }} as throughput_units_per_hour,

        {{ utilization_status(
            utilization_ratio('total_units_planned', 'capacity_per_day')
        ) }} as utilization_status

    from metrics
)

select * from final
=======
        install_date,
        coalesce(total_orders, 0) as total_orders,
        coalesce(total_units_planned, 0) as total_units_planned,
        capacity_per_day,
        total_production_hours,
        available_hours,
        round(greatest(0, available_hours - total_production_hours), 2) as idle_hours,
        {{ pct('total_units_planned', 'capacity_per_day') }} as utilization_rate_pct,
        {{ throughput_units_per_hour('total_units_planned', 'total_production_hours') }} as throughput_units_per_hour,
        {{ utilization_status('total_units_planned / nullif(capacity_per_day,0)') }} as utilization_status,
        case
            when install_date is null then 'Unknown'
            when datediff('day', install_date, current_date()) <= 365 then 'Commissioned'
            when datediff('day', install_date, current_date()) <= 3 * 365 then 'Mid-Life'
            when datediff('day', install_date, current_date()) <= 6 * 365 then 'Aging'
            else 'End-of-Life'
        end as lifecycle_stage
    from metrics_calculation
)

<<<<<<< HEAD
SELECT * FROM final
>>>>>>> 5a283c115a78f476192decb4c80fce057f5a28cc
=======
select * from final
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
