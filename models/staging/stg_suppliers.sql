{{ config(
    materialized = 'incremental',
    unique_key = 'supplier_id',
    on_schema_change = 'sync_all_columns'
) }}

with base as (
    select
        supplier_id,
        supplier_name,
        contact_name,
        phone,
        country
    from {{ source('src', 'raw_supplier') }}
),

filtered as (

    {% if is_incremental() %}
        select *
        from base
        where supplier_id not in (select supplier_id from {{ this }})
    {% else %}
        select * from base
    {% endif %}

),

renamed as (
    select
        trim(supplier_id)                as supplier_id,
        initcap(trim(supplier_name))     as supplier_name,
        initcap(trim(contact_name))      as contact_name,
        trim(phone)                      as phone,
        upper(trim(country))             as country,
        current_timestamp()              as record_loaded_ts
    from filtered
)

select * from renamed
