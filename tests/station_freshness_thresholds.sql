WITH stats AS (
    SELECT
        COUNT(*) AS total_stations,
        COUNT(*) FILTER (WHERE freshness_status = 'dead (>24hr)') AS dead_stations,
        ROUND(COUNT(*) FILTER (WHERE freshness_status = 'dead (>24hr)') * 100.0 / COUNT(*), 2) AS dead_pct
    FROM {{ ref('station_freshness') }}
)
SELECT
    total_stations,
    dead_stations,
    dead_pct,
    'More than 5% of stations are dead' AS failure_reason
FROM stats
WHERE dead_pct > 5
