SELECT
    u.*
FROM (
    SELECT UNNEST(items) AS u
    FROM read_json('https://environment.data.gov.uk/flood-monitoring/id/stations')
)
