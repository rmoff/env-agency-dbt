{{
    config(
        materialized='incremental',
        unique_key=['reading_date', 'measure']
    )
}}

SELECT dateTime::DATE AS reading_date,
    measure,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    MEDIAN(value) AS median_value,
    COUNT(*) AS reading_ct,
    MIN(dateTime) AS earliest_reading_dateTime,
    MAX(dateTime) AS latest_reading_dateTime,
    ANY_VALUE(datumType) AS datumType,
    ANY_VALUE(m_label) AS m_label,
    ANY_VALUE(parameter) AS parameter,
    ANY_VALUE(parameterName) AS parameterName,
    ANY_VALUE(period) AS period,
    ANY_VALUE(qualifier) AS qualifier,
    ANY_VALUE(unit) AS unit,
    ANY_VALUE(unitName) AS unitName,
    ANY_VALUE(valueType) AS valueType,
    ANY_VALUE(station) AS station,
    ANY_VALUE(RLOIid) AS RLOIid,
    ANY_VALUE(catchmentName) AS catchmentName,
    ANY_VALUE(dateOpened) AS dateOpened,
    ANY_VALUE(easting) AS easting,
    ANY_VALUE(s_label) AS s_label,
    ANY_VALUE(latitude) AS latitude,
    ANY_VALUE(longitude) AS longitude,
    ANY_VALUE(northing) AS northing,
    ANY_VALUE(riverName) AS riverName,
    ANY_VALUE(stageScale) AS stageScale,
    ANY_VALUE(stationReference) AS stationReference,
    ANY_VALUE(status) AS status,
    ANY_VALUE(town) AS town,
    ANY_VALUE(wiskiID) AS wiskiID,
    ANY_VALUE(datumOffset) AS datumOffset,
    ANY_VALUE(gridReference) AS gridReference,
    ANY_VALUE(downstageScale) AS downstageScale
FROM {{ ref('obt_readings') }}

{% if is_incremental() %}
    WHERE reading_date >= (SELECT MAX(reading_date) FROM {{ this }})
{% endif %}

GROUP BY reading_date, measure
