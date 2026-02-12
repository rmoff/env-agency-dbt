SELECT
    u.* EXCLUDE (measure),
    {{ strip_api_url('u.measure', 'measures') }} AS measure
FROM (
    SELECT UNNEST(items) AS u
    FROM read_json('https://environment.data.gov.uk/flood-monitoring/data/readings?latest')
)
