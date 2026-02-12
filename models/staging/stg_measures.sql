SELECT
    u.* EXCLUDE (station),
    {{ strip_api_url('u.station', 'stations') }} AS station
FROM (
    SELECT UNNEST(items) AS u
    FROM read_json('https://environment.data.gov.uk/flood-monitoring/id/measures')
)
