with machines as (
SELECT
    MACHINE_ID   AS machine_id,
    MACHINE_TYPE AS machine_type,
<<<<<<< HEAD
    CAPACITY_PER_DAY  AS capacity_per_day,
=======
   CAPACITY_PER_DAY   AS capacity_per_day,
>>>>>>> 2c6e9a7cbcb6691a1edab09b1423d3d03473c2e9
    INSTALL_DATE AS install_date,
    STATUS       AS status
FROM {{ source('src', 'raw_machine') }}
)
select * from machines