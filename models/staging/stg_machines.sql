with machines as (
SELECT
    MACHINE_ID   AS machine_id,
    MACHINE_TYPE AS machine_type,
    CAPACITY_PER_DAY   AS capacity_per_day,
    INSTALL_DATE AS install_date,
    STATUS       AS status
FROM {{ source('src', 'raw_machine') }}
)
select * from machines