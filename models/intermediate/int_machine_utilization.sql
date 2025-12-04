with machines as (

    select
        upper(trim(machine_id)) as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        status as current_machine_status
    from {{ ref('stg_machines') }}

),

orders as (

    select
        upper(trim(machine_id)) as join_key,
        machine_id as raw_order_machine_id,
        start_date as production_date,
        count(production_order_id) as total_orders,
        sum(planned_quantity) as total_units_planned
    from {{ ref('stg_production_orders') }}
    group by 1, 2, 3

),

joined_data as (

    select
        orders.production_date,
        coalesce(machines.machine_id, orders.raw_order_machine_id) as machine_id,
        machines.machine_type,
        machines.current_machine_status as machine_status,
        orders.total_orders,
        orders.total_units_planned,
        machines.capacity_per_day,

        case 
            when machines.machine_type in ('Drill', 'Lathe', 'Milling') then 'Standard Machining'
            when machines.machine_type = 'CNC' then 'Advanced Machining'
            when machines.machine_type = 'Laser Cutter' then 'Fabrication'
            when machines.machine_type = '3D Printer' then 'Additive Manufacturing'
            else 'Other'
        end as department

    from orders
    left join machines 
        on orders.join_key = machines.join_key

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
        capacity_per_day

    from joined_data

)

select * from final