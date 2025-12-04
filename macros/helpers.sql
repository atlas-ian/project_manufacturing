<<<<<<< HEAD
--hello

-- macros/manufacturing_metrics.sql


=======
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc

{% macro clean_id(column_name) %}
    upper(trim({{ column_name }}))
{% endmacro %}

{% macro map_machine_department(machine_type_expr) %}
    case 
        when {{ machine_type_expr }} in ('Drill', 'Lathe', 'Milling') then 'Standard Machining'
        when {{ machine_type_expr }} in ('CNC')                    then 'Advanced Machining'
        when {{ machine_type_expr }} = 'Laser Cutter'              then 'Fabrication'
        when {{ machine_type_expr }} = '3D Printer'                then 'Additive Manufacturing'
        else 'Other'
    end
{% endmacro %}



{% macro safe_divide(numerator, denominator) %}
    ( {{ numerator }} / nullif({{ denominator }}, 0) )
{% endmacro %}

{% macro pct(numerator, denominator) %}
    round( {{ numerator }} / nullif({{ denominator }}, 0) * 100, 2 )
{% endmacro %}

{% macro safe_round(expr, scale=2) %}
    round({{ expr }}, {{ scale }})
{% endmacro %}



{% macro utilization_ratio(total_units_expr, capacity_per_day_expr) %}
    {{ safe_divide(total_units_expr, capacity_per_day_expr) }}
{% endmacro %}

{% macro utilization_status(load_ratio_expr) %}
    case
        when {{ load_ratio_expr }} is null               then 'Unknown Capacity'
        when {{ load_ratio_expr }} > 1.0                 then 'Overloaded'
        when {{ load_ratio_expr }} >= 0.8                then 'Optimal'
        when {{ load_ratio_expr }} >= 0.5                then 'Underutilized'
        else 'Idle/Low'
    end
{% endmacro %}

{% macro throughput_units_per_hour(units_expr, hours_expr) %}
    round( {{ safe_divide(units_expr, hours_expr) }}, 2 )
{% endmacro %}



{% macro duration_days(start_date_expr, end_date_expr) %}
    greatest(1, datediff(day, {{ start_date_expr }}, {{ end_date_expr }}))
{% endmacro %}

{% macro production_hours(start_date_expr, end_date_expr) %}
    {{ duration_days(start_date_expr, end_date_expr) }} * 24
{% endmacro %}

{% macro efficiency_score_pct(planned_qty_expr, capacity_per_day_expr, duration_days_expr) %}
    round(
        {{ planned_qty_expr }} / nullif({{ capacity_per_day_expr }} * {{ duration_days_expr }}, 0) * 100,
        2
    )
{% endmacro %}

{% macro efficiency_status(efficiency_pct_expr) %}
    case
        when {{ efficiency_pct_expr }} > 100 then 'Over-Capacity / Data Error'
        when {{ efficiency_pct_expr }} >= 80 then 'High Efficiency'
        when {{ efficiency_pct_expr }} >= 50 then 'Normal Load'
        when {{ efficiency_pct_expr }} >  0 then 'Low Utilization'
        else 'No Production'
    end
{% endmacro %}



{% macro fulfillment_rate_pct(qty_shipped_expr, total_planned_expr) %}
    {{ pct(qty_shipped_expr, total_planned_expr) }}
{% endmacro %}

{% macro transit_days(ship_date_expr, delivery_date_expr) %}
    datediff(day, {{ ship_date_expr }}, {{ delivery_date_expr }})
{% endmacro %}

{% macro delay_days(planned_completion_expr, ship_date_expr) %}
    greatest(0, datediff(day, {{ planned_completion_expr }}, {{ ship_date_expr }}))
{% endmacro %}

{% macro shipment_timing_status(ship_date_expr, planned_completion_expr) %}
    case 
        when {{ ship_date_expr }} < {{ planned_completion_expr }} then 'Shipped from Inventory'
        when {{ ship_date_expr }} > {{ planned_completion_expr }} then 'Production Lag'
        else 'Just-in-Time'
    end
{% endmacro %}



{% macro inventory_status(on_hand_expr) %}
    case
        when {{ on_hand_expr }} = 0        then 'Critical: Stockout'
        when {{ on_hand_expr }} < 100      then 'Warning: Low Stock'
        when {{ on_hand_expr }} > 800      then 'Flag: Potential Overstock'
        else 'Healthy'
    end
{% endmacro %}

{% macro inventory_recommended_action(on_hand_expr, on_order_expr) %}
    case
        when {{ on_hand_expr }} = 0 and {{ on_order_expr }} = 0 then 'Order Immediately'
        when {{ on_hand_expr }} = 0 and {{ on_order_expr }} > 0 then 'Expedite Shipment'
        when {{ on_hand_expr }} < 100 and {{ on_order_expr }} = 0 then 'Reorder Soon'
        when {{ on_hand_expr }} > 800 then 'Stop Ordering / Promo'
        else 'No Action'
    end
<<<<<<< HEAD
{% endmacro %}
=======
{% endmacro %}
>>>>>>> e1b149b868a525c032773b0166018e564b2fbedc
