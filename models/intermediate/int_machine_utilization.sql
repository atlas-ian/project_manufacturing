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

),

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
