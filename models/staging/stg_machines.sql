with machines as (
SELECT
    MACHINE_ID   AS machine_id,
    MACHINE_TYPE AS machine_type,
<<<<<<< HEAD
    CAPACITY_PER_DAY   AS capacity_per_day,
=======
   CAPACITY_PER_DAY   AS capacity_per_day,
>>>>>>> 5a283c115a78f476192decb4c80fce057f5a28cc
    INSTALL_DATE AS install_date,
    STATUS       AS status
FROM {{ source('src', 'raw_machine') }}
)
select * from machines