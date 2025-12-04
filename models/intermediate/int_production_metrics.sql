{{ config(materialized='table') }}

with machines as (

    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as machine_current_status
    from {{ ref('stg_machines') }}

),

orders as (

    select
        {{ clean_id('machine_id') }} as join_key,
        production_order_id,
        product_id,
        planned_quantity,
        start_date,
        end_date,
        status as order_status
    from {{ ref('stg_production_orders') }}

),

joined_data as (

    select
        o.production_order_id,
        o.product_id,
        m.machine_id,
        o.order_status,
        m.machine_current_status,
        {{ map_machine_department('m.machine_type') }} as department,
        m.machine_type,
        o.planned_quantity,
        m.capacity_per_day,
        o.start_date,
        o.end_date
    from orders o
    inner join machines m
        on o.join_key = m.join_key

),

calculations as (

    select
        *,
        {{ duration_days('start_date', 'end_date') }} as duration_days,
        {{ production_hours('start_date', 'end_date') }} as production_hours
    from joined_data

),

metrics as (

    select
        *,
        {{ throughput_units_per_hour('planned_quantity', 'production_hours') }} as throughput_units_per_hour,
        {{ efficiency_score_pct('planned_quantity', 'capacity_per_day', 'duration_days') }} as efficiency_score_pct
    from calculations

),

final as (

    select
        production_order_id,
        product_id,
        machine_id,
        department,
        machine_type,
        order_status,
        machine_current_status,
        planned_quantity,
        production_hours,
        throughput_units_per_hour,
        efficiency_score_pct,
        {{ efficiency_status('efficiency_score_pct') }} as efficiency_status,
        start_date,
        end_date
    from metrics

)

select * from final
