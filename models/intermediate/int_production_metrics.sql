{{ config(
    materialized='table'
) }}

with machines as (
    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as machine_current_status
    from {{ source('src', 'raw_machine') }}
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
    from {{ source('src', 'raw_production_order') }}
),

joined_data as (
    select
        orders.production_order_id,
        orders.product_id,
        machines.machine_id,
        orders.order_status,
        machines.machine_current_status,
        machines.machine_type,
        orders.planned_quantity,
        machines.capacity_per_day,
        orders.start_date,
        orders.end_date,
        -- Department mapping using macro
        {{ map_machine_department('machines.machine_type') }} as department
    from orders
    inner join machines
        on orders.join_key = machines.join_key
),

calculations as (
    select
        *,
        -- Duration in days
        {{ duration_days('start_date', 'end_date') }} as duration_days,
        -- Production hours
        {{ production_hours('start_date', 'end_date') }} as production_hours
    from joined_data
),

metrics as (
    select
        *,
        -- Throughput units per hour using macro
        {{ throughput_units_per_hour('planned_quantity', 'production_hours') }} as throughput_units_per_hour,
        -- Efficiency % using macro
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
        -- Efficiency status using macro
        {{ efficiency_status('efficiency_score_pct') }} as efficiency_status,
        start_date,
        end_date
    from metrics
)

select * from final
