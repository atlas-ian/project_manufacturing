{% snapshot supplier_scd %}

{{
  config(
    target_database = 'ANALYTICS',
    target_schema   = 'MANUFACTURING360_SCD',
    unique_key      = 'supplier_id',
    strategy        = 'check',
    check_cols      = ['supplier_name', 'contact_name', 'phone', 'country']
  )
}}

select
  supplier_id,
  supplier_name,
  contact_name,
  phone,
  country
from {{ ref('stg_suppliers') }}

{% endsnapshot %}