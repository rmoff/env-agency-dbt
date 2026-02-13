SELECT CAST(date_day AS DATE) AS date_day,
    year(date_day) AS date_year,
    month(date_day) AS date_month,
    monthname(date_day) AS date_monthname,
    dayofmonth(date_day) AS date_dayofmonth,
    dayofweek(date_day) AS date_dayofweek,
    CAST(CASE WHEN dayofweek(date_day) IN (0,6) THEN 1 ELSE 0 END AS BOOLEAN) AS date_is_weekend,
    dayname(date_day) AS date_dayname,
    dayofyear(date_day) AS date_dayofyear,
    weekofyear(date_day) AS date_weekofyear,
    quarter(date_day)  AS date_quarter
FROM (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2031-01-01' as date)"
        ) }}
    ) AS date_spine
