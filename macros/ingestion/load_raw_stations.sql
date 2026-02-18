{% macro load_raw_stations() %}
{% set endpoint = var('api_base_url') ~ '/id/stations' %}
{% do log("raw_stations ~ reading from " ~ endpoint, info=true) %}

{% set sql %}
    CREATE OR REPLACE TABLE raw_stations AS
    SELECT *,
            CURRENT_TIMESTAMP
            AS _latest_reading_at
    FROM read_json('{{ endpoint }}')
{% endset %}
{% do run_query(sql) %}

{% do log("raw_stations ~ loaded", info=true) %}

{% endmacro %}
