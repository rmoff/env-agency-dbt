{% macro strip_api_url(column, entity) %}
    REGEXP_REPLACE(
        {{ column }},
        'http://environment\.data\.gov\.uk/flood-monitoring/id/{{ entity }}/',
        ''
    )
{% endmacro %}
