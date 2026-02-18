{% macro load_raw_measures() %}
{% set endpoint = var('api_base_url') ~ '/id/measures' %}
{% do log("raw_measures ~ reading from " ~ endpoint, info=true) %}

{% set sql %}
    CREATE OR REPLACE TABLE raw_measures AS
    SELECT *,
            CURRENT_TIMESTAMP
            AS _latest_reading_at
    FROM read_json('{{ endpoint }}')
{% endset %}
{% do run_query(sql) %}

{% do log("raw_measures ~ loaded", info=true) %}

{% endmacro %}
