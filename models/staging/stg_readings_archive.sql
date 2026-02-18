SELECT
    dateTime,
    {{ strip_api_url('measure', 'measures') }} AS measure,
    -- Handle pipe-delimited values: take first, cast to double
    CAST(
        CASE
            WHEN value LIKE '%|%' THEN split_part(value, '|', 1)
            ELSE value
        END AS DOUBLE
    ) AS value
FROM {{ source('env_agency', 'raw_readings_archive') }}