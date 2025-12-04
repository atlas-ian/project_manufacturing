{% snapshot product_scd %}

{{
  config(
    target_database = 'ANALYTICS',
    target_schema   = 'MANUFACTURING360_SCD',
    unique_key      = 'product_id',
    strategy        = 'check',
    check_cols      = ['product_name', 'category', 'supplier_id', 'unit_of_measure', 'price', 'weight_kg']
  )
}}

select
  product_id,
  product_name,
  category,
  supplier_id,
  unit_of_measure,
  price,
  weight_kg
from {{ ref('stg_products') }}

{% endsnapshot %}
