{% test max_pct_failing(model, column, failing_value, threshold_pct) %}

WITH stats AS (
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE {{ column }} = '{{ failing_value }}') AS failing,
        ROUND(
            COUNT(*) FILTER (WHERE {{ column }} = '{{ failing_value }}') * 100.0 / COUNT(*),
            2
        ) AS failing_pct
    FROM {{ model }}
)
SELECT
    total,
    failing,
    failing_pct,
    {{ threshold_pct }} AS threshold_pct,
    'Failing pct ' || failing_pct || '% exceeds threshold ' || {{ threshold_pct }} || '%' AS failure_reason
FROM stats
WHERE failing_pct > {{ threshold_pct }}

{% endtest %}
