
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

