with machines as (

    select
        upper(trim(machine_id)) as join_key,
        machine_id,
        machine_type,
        capacity_per_day,
        -- FIX: Added Machine Status from source
        status as machine_current_status
    from {{ ref('stg_machines') }}

),

orders as (

    select
        upper(trim(machine_id)) as join_key,
        production_order_id,
        product_id,
        planned_quantity,
        start_date,
        end_date,
        -- FIX: Renamed for clarity vs machine status
        status as order_status
    from {{ ref('stg_production_orders') }}

),

joined_data as (

    select
        orders.production_order_id,
        orders.product_id,
        machines.machine_id,
        
        -- Status Columns (Replacing Shift ID)
        orders.order_status,
        machines.machine_current_status,
        
        case 
            when machines.machine_type in ('Drill', 'Lathe', 'Milling') then 'Standard Machining'
            when machines.machine_type in ('CNC') then 'Advanced Machining'
            when machines.machine_type = 'Laser Cutter' then 'Fabrication'
            when machines.machine_type = '3D Printer' then 'Additive Manufacturing'
            else 'Other'
        end as department,

        machines.machine_type,
        orders.planned_quantity,
        machines.capacity_per_day,
        orders.start_date,
        orders.end_date

    from orders
    inner join machines 
        on orders.join_key = machines.join_key

),

calculations as (

    select
        *,
        -- Calculate Duration (Minimum 1 day to prevent 0 division)
        greatest(1, datediff(day, start_date, end_date)) as duration_days,

        -- Production Hours (Duration * 24)
        greatest(1, datediff(day, start_date, end_date)) * 24 as production_hours

    from joined_data

),

metrics as (

    select
        *,
        
        -- Throughput (Units / Hour)
        round(
            planned_quantity / nullif(production_hours, 0), 
        2) as throughput_units_per_hour,

        -- Efficiency % calculation
        round(
            (planned_quantity / nullif( (capacity_per_day * duration_days), 0 )) * 100, 
        2) as efficiency_score_pct

    from calculations

),

final as (

    select
        production_order_id,
        product_id,
        machine_id,
        department,
        machine_type,
        
        -- REPLACED: shift_id removed, Order Status added
        order_status,
        machine_current_status,
        
        planned_quantity,
        production_hours,
        throughput_units_per_hour,
        efficiency_score_pct,
        
        -- Calculated Efficiency Status
        case
            when efficiency_score_pct > 100 then 'Over-Capacity / Data Error'
            when efficiency_score_pct >= 80 then 'High Efficiency'
            when efficiency_score_pct >= 50 then 'Normal Load'
            when efficiency_score_pct > 0 then 'Low Utilization'
            else 'No Production'
        end as efficiency_status,

        start_date,
        end_date

    from metrics

)

select * from final