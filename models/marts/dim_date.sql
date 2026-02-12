SELECT CAST(range AS DATE) AS date_day,
    year(range) AS date_year,
    month(range) AS date_month,
    monthname(range) AS date_monthname,
    dayofmonth(range) AS date_dayofmonth,
    dayofweek(range) AS date_dayofweek,
    CAST(CASE WHEN dayofweek(range) IN (0,6) THEN 1 ELSE 0 END AS BOOLEAN) AS date_is_weekend,
    dayname(range) AS date_dayname,
    dayofyear(range) AS date_dayofyear,
    weekofyear(range) AS date_weekofyear,
    quarter(range)  AS date_quarter
FROM range(DATE '2020-01-01', DATE '2031-01-01', INTERVAL '1 day')
