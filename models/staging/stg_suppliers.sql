with suppliers as (
    select * from {{ source('src','raw_supplier')}}
)

select * from suppliers