SELECT
    u.*
FROM (
    SELECT UNNEST(items) AS u
    FROM {{ source('env_agency', 'raw_stations') }}
)
