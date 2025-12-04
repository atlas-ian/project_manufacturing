with machine_daily_data as (

    select
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
        total_orders,
        total_units_planned,
        capacity_per_day
    from {{ ref('int_machine_utilization') }}

),

metrics_calculation as (

    select
        *,
        24 as available_hours,

        round(
            (total_units_planned / nullif(capacity_per_day, 0)) * 24,
        2) as total_production_hours

    from machine_daily_data

),

final as (

    select
        machine_id,
        production_date,
        department,
        machine_type,
        machine_status,
        total_orders,
        total_units_planned,
        capacity_per_day,

        -- Time Metrics
        total_production_hours,
        available_hours,
        round(greatest(0, available_hours - total_production_hours), 2) as idle_hours,

        -- Utilization Rate
        round(
            (total_production_hours / nullif(available_hours, 0)) * 100,
        2) as utilization_rate_pct,

        -- Utilization Status
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