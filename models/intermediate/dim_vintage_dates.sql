WITH dim_date AS (
    SELECT
        time_stamp,
        time_interval,
        last_refreshed,
        time_zone,
        EXTRACT(YEAR FROM time_stamp) AS year,
        EXTRACT(MONTH FROM time_stamp) AS month,
        EXTRACT(DAY FROM time_stamp) AS day,
        FORMAT_DATE('%A', time_stamp) AS weekday,
        EXTRACT(QUARTER FROM time_stamp) AS quarter,
        EXTRACT(WEEK FROM time_stamp) AS week_number,
        CASE WHEN EXTRACT(DAYOFWEEK FROM time_stamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS weekday_weekend,
        CASE WHEN EXTRACT(DAY FROM time_stamp) IN (25, 26, 31) AND EXTRACT(MONTH FROM time_stamp) = 12 THEN 'Holiday' ELSE 'Non-Holiday' END AS holiday,
        IF(EXTRACT(MONTH FROM time_stamp) = 2 AND MOD(EXTRACT(YEAR FROM time_stamp), 4) = 0, 'Leap Year', 'Non-Leap Year') AS leap_year,
        CASE
        WHEN EXTRACT(MONTH FROM time_stamp) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM time_stamp) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM time_stamp) IN (6, 7, 8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM time_stamp) IN (9, 10, 11) THEN 'Fall'
        ELSE 'Unknown'
        END AS season
    FROM {{ ref("stg_vintage") }}
)

SELECT DISTINCT *
FROM dim_date
