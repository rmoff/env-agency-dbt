SELECT
    s.notation,
    s.label,
      MAX(r.dateTime) AS last_reading_at,
      CURRENT_TIMESTAMP - MAX(r.dateTime) AS time_since_last_reading,
      CASE
          WHEN MAX(r.dateTime) > CURRENT_TIMESTAMP - INTERVAL '1 hours' THEN 'fresh (<1hr)'
          WHEN MAX(r.dateTime) > CURRENT_TIMESTAMP - INTERVAL '24 hours' THEN 'stale (<24hr)'
          ELSE 'dead (>24hr)'
      END AS freshness_status
  FROM {{ ref('dim_stations') }} s
  LEFT JOIN {{ ref('dim_measures') }} m ON m.station = s.notation
  LEFT JOIN {{ ref('fct_readings') }} r ON r.measure = m.notation
  GROUP BY s.notation, s.label
