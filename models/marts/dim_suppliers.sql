{{ config(materialized='table') }}

with src as (
    select * from {{ ref('int_supplier_metrics') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        supplier_id,
        supplier_name,
        country,
        contact_name,
        phone,
        supplier_risk,
        phone_validity
    from src
)

select * from final;
