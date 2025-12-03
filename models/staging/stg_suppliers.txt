with suppliers as (
    select * from {{ source('src','stg_supplier')}}
)

select * from suppliers