{% macro load_raw_readings() %}
{% set endpoint = var('api_base_url') ~ '/data/readings?latest' %}

{% do log("raw_readings ~ reading from " ~ endpoint, info=true) %}

{% set sql %}
    CREATE OR REPLACE TABLE raw_readings AS
    SELECT *,
            list_max(list_transform(items, x -> x.dateTime))
            AS _latest_reading_at
    FROM read_json('{{ endpoint }}')
{% endset %}
{% do run_query(sql) %}

{% do log("raw_readings ~  loaded", info=true) %}

{% endmacro %}
