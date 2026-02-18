{% macro backfill_readings(start_date, end_date) %}

{# Validate date range #}
{% if start_date > end_date %}
    {{ exceptions.raise_compiler_error("start_date (" ~ start_date ~ ") must be before end_date (" ~ end_date ~ ")") }}
{% endif %}

{# 1. Create table with explicit schema (no network call) #}
{% set create_sql %}
CREATE TABLE IF NOT EXISTS raw_readings_archive (
    dateTime TIMESTAMP WITH TIME ZONE,
    measure VARCHAR,
    value VARCHAR
)
{% endset %}
{% do run_query(create_sql) %}

{# 2. Delete target date range (idempotent) #}
{% set delete_sql %}
DELETE FROM raw_readings_archive
WHERE dateTime::DATE BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
{% endset %}
{% do run_query(delete_sql) %}
{% do log("Cleared " ~ start_date ~ " to " ~ end_date, info=true) %}

{# 3. Load all dates in one statement using DuckDB list comprehension #}
{% set load_sql %}
INSERT INTO raw_readings_archive
SELECT * FROM read_csv(
    list_transform(
        generate_series(DATE '{{ start_date }}', DATE '{{ end_date }}', INTERVAL 1 DAY),
        d -> 'https://environment.data.gov.uk/flood-monitoring/archive/readings-' || strftime(d, '%Y-%m-%d') || '.csv'
    ),
    header = true,
    timestampformat = '%Y-%m-%dT%H:%M:%SZ',
    columns = {
        'dateTime': 'TIMESTAMP',
        'measure': 'VARCHAR',
        'value': 'VARCHAR'
    }
)
{% endset %}
{% do run_query(load_sql) %}
{% do log("Loaded " ~ start_date ~ " to " ~ end_date, info=true) %}

{% endmacro %}
