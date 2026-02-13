SELECT
    u.* EXCLUDE (measure),
    {{ strip_api_url('u.measure', 'measures') }} AS measure
FROM (
    SELECT UNNEST(items) AS u
    FROM {{ source('env_agency', 'raw_readings') }}
)
