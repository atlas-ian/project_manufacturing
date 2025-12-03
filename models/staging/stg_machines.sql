with machines as (
SELECT
    MACHINE_ID   AS machine_id,
    MACHINE_TYPE AS machine_type,
    DEPARTMENT   AS department,
    INSTALL_DATE AS install_date,
    STATUS       AS status
FROM {{ source('src', 'raw_machine') }}
)
select * from machines