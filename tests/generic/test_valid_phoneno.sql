
select *
from {{ ref('stg_suppliers') }}
where phone not regexp '^[0-9\-+\s]+$'
