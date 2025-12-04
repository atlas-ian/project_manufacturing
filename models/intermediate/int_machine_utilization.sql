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
        machines.capacity_per_day,
        {{ map_machine_department('machines.machine_type') }} as department
    from orders
    left join machines
        on orders.join_key = machines.join_key
),

metrics_calculation as (
    select
        *,
        24 as available_hours,
        -- Estimate production hours based on capacity
        case 
            when capacity_per_day > 0 then (total_units_planned / capacity_per_day) * 24
            else 0
        end as total_production_hours
    from joined_data
),

final as (
    select
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
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

select * from final
