SELECT
    u.dateTime,
    {{ strip_api_url('u.measure', 'measures') }} AS measure,
    CAST(
        CASE WHEN json_type(u.value) = 'ARRAY' THEN u.value->>0
             ELSE CAST(u.value AS VARCHAR)
        END AS DOUBLE
    ) AS value
FROM (
    SELECT UNNEST(items) AS u
    FROM {{ source('env_agency', 'raw_readings') }}
)
