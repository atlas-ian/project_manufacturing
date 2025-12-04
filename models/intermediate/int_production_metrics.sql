<<<<<<< HEAD
{{ config(materialized='table') }}

with machines as (
=======
{{ config(
    materialized='table'
) }}
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc

with machines as (
    select
        {{ clean_id('machine_id') }} as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as machine_current_status
<<<<<<< HEAD
    from {{ ref('stg_machines') }}

=======
    from {{ source('src', 'raw_machine') }}
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
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
<<<<<<< HEAD
    from {{ ref('stg_production_orders') }}

=======
    from {{ source('src', 'raw_production_order') }}
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
),

joined_data as (
    select
<<<<<<< HEAD
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

=======
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
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
),

calculations as (
    select
        *,
<<<<<<< HEAD
        {{ duration_days('start_date', 'end_date') }} as duration_days,
=======
        -- Duration in days
        {{ duration_days('start_date', 'end_date') }} as duration_days,
        -- Production hours
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
        {{ production_hours('start_date', 'end_date') }} as production_hours
    from joined_data
),

metrics as (
    select
        *,
<<<<<<< HEAD
        {{ throughput_units_per_hour('planned_quantity', 'production_hours') }} as throughput_units_per_hour,
=======
        -- Throughput units per hour using macro
        {{ throughput_units_per_hour('planned_quantity', 'production_hours') }} as throughput_units_per_hour,
        -- Efficiency % using macro
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
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
<<<<<<< HEAD
=======
        -- Efficiency status using macro
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
        {{ efficiency_status('efficiency_score_pct') }} as efficiency_status,
        start_date,
        end_date
    from metrics
)

select * from final
