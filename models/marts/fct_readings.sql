{{
    config(
        materialized='incremental',
        unique_key=['dateTime', 'measure']
    )
}}

SELECT
    dateTime,
    measure,
    -- API sometimes returns multiple values as JSON array; take first value
    -- e.g.
    -- 🟡◗ select len(value),value,measure from stg_readings order by 1 desc;
    --     ;
    -- ┌──────────────┬─────────────────┬───────────────────────────────────────────────────────┐
    -- │ len("value") │      value      │                        measure                        │
    -- │    int64     │      json       │                        varchar                        │
    -- ├──────────────┼─────────────────┼───────────────────────────────────────────────────────┤
    -- │           15 │ [-52.984,1.016] │ 1674TH-level-stage-i-15_min-mASD                      │
    -- │           13 │ [3.986,4.267]   │ E6619-flow--i-15_min-m3_s                             │
    CAST(
        CASE
            WHEN json_type(value) = 'ARRAY' THEN value->>0
            ELSE CAST(value AS VARCHAR)
        END AS DOUBLE
    ) AS value
FROM
    {{ ref('stg_readings') }}

{% if is_incremental() %}
    WHERE dateTime > (SELECT MAX(dateTime) FROM {{ this }})
{% endif %}
