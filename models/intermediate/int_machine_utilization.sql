{{ config(
    on_schema_change = 'sync_all_columns'
) }}

-- ===============================================================
-- Intermediate Model: int_machine_utilization
-- Purpose: Aggregate machine utilization, availability, and performance
-- ===============================================================

with machine as (
    select
        machine_id,
        machine_type,
        department,
        status as machine_status
    from {{ ref('stg_machines') }}
),

production as (
    select
        machine_id,
        date_trunc('day', start_date) as production_date,
        count(distinct production_order_id) as total_orders,
        sum(planned_quantity) as total_units_planned,
        datediff('hour', min(start_date), max(end_date)) as total_production_hours
    from {{ ref('stg_production_orders') }}
    where machine_id is not null
    group by machine_id, date_trunc('day', start_date)
),

-- Assume each machine is available for 24 hours daily
utilization_calc as (
    select
        p.machine_id,
        p.production_date,
        m.department,
        m.machine_type,
        m.machine_status,
        p.total_orders,
        p.total_units_planned,
        coalesce(p.total_production_hours, 0) as total_production_hours,
        24 as available_hours,
        24 - coalesce(p.total_production_hours, 0) as idle_hours,
        round(coalesce(p.total_production_hours, 0) / 24 * 100, 2) as utilization_rate_pct,
        case
            when coalesce(p.total_production_hours, 0) > 0
            then round(p.total_units_planned / p.total_production_hours, 2)
            else 0
        end as throughput_units_per_hour,
        case
            when round(coalesce(p.total_production_hours, 0) / 24 * 100, 2) >= 85 then 'High'
            when round(coalesce(p.total_production_hours, 0) / 24 * 100, 2) between 60 and 84 then 'Medium'
            else 'Low'
        end as utilization_status
    from production p
    left join machine m using (machine_id)
)

select * from utilization_calc

{% if is_incremental() %}
where production_date >= dateadd('day', -1, current_date())
{% endif %}

