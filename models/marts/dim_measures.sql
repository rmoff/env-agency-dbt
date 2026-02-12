SELECT
    notation,
    datumType,
    label,
    notation,
    parameter,
    parameterName,
    period,
    qualifier,
    stationReference,
    unit,
    unitName,
    valueType,
    station
FROM
    {{ ref('snap_measures') }}
WHERE dbt_valid_to IS NULL
