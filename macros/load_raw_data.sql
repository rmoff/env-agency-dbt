{% macro load_raw_data() %}

    {% set sources = [
        ('raw_readings', '/data/readings?latest'),
        ('raw_stations', '/id/stations'),
        ('raw_measures', '/id/measures')
    ] %}

    {% for table_name, api_endpoint in sources %}
        {% set sql %}
            CREATE OR REPLACE TABLE {{ table_name }} AS
            SELECT *,
                    {% if table_name == 'raw_readings' %}
                    list_max(list_transform(items, x -> x.dateTime))
                    {% else %}
                        CURRENT_TIMESTAMP
                    {% endif %}
                    AS _latest_reading_at
            FROM read_json('{{ var('api_base_url') }}{{ api_endpoint }}')
        {% endset %}

        {% do run_query(sql) %}
        {% do log(table_name ~ " loaded", info=true) %}
    {% endfor %}

{% endmacro %}
