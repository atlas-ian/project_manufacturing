{% snapshot machine_scd %}

{{
  config(
    target_database='ANALYTICS',
    target_schema='MANUFACTURING360_SCD',
    unique_key='machine_id',
    strategy='check',
    check_cols=['machine_type', 'capacity_per_day', 'status']
  )
}}

select
  machine_id,
  machine_type,
  capacity_per_day,
  install_date,
  status
from {{ ref('stg_machines') }}

{% endsnapshot %}