{{
    config(
        materialized='incremental',
        unique_key=['dateTime', 'measure']
    )
}}

SELECT * FROM {{ ref('stg_readings') }}
UNION ALL
SELECT * FROM {{ ref('stg_readings_archive') }}

{% if is_incremental() %}
WHERE dateTime > (SELECT MAX(dateTime) FROM {{ this }})
{% endif %}
