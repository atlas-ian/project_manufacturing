{{ config(
    materialized = 'incremental',
    unique_key = 'supplier_id',
    on_schema_change = 'sync_all_columns'
) }}

with source as (
    select
        supplier_id,
        supplier_name,
        contact_name,
        phone,
        country,
        current_timestamp() as record_loaded_ts
    from {{ source('src', 'raw_supplier') }}

    {% if is_incremental() %}
        -- Load only new or updated records
        where record_loaded_ts > (select coalesce(max(record_loaded_ts), '1900-01-01') from {{ this }})
    {% endif %}

),

renamed as (

    select
        trim(supplier_id)                as supplier_id,
        initcap(trim(supplier_name))     as supplier_name,
        initcap(trim(contact_name))      as contact_name,
        trim(phone)                      as phone,
        upper(trim(country))             as country,
        record_loaded_ts
    from source

)

select * from renamed

