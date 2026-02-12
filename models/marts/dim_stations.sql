-- API sometimes returns multiple items as JSON array; take first value
SELECT
    notation,
    CAST(RLOIid->>0 AS VARCHAR) AS RLOIid,
    CAST(catchmentName AS VARCHAR) AS catchmentName,
    CAST(dateOpened->>0 AS DATE) AS dateOpened,
    CAST(easting->>0 AS INTEGER) AS easting,
    CAST(label AS VARCHAR) AS label,
    CAST(lat->>0 AS DOUBLE) AS latitude,
    CAST("long"->>0 AS DOUBLE) AS longitude,
    CAST(northing->>0 AS INTEGER) AS northing,
    riverName,
    stageScale,
    stationReference,
    CAST(status AS VARCHAR) AS status,
    town,
    wiskiID,
    datumOffset,
    gridReference,
    downstageScale
FROM
    {{ ref('snap_stations') }}
WHERE dbt_valid_to IS NULL
