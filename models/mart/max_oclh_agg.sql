WITH oclh AS (
    SELECT
        time_stamp,
        EXTRACT(DAY FROM time_stamp) AS day,
        open,
        close,
        low,
        high
    FROM {{ ref("fact_stock_prices") }}

)

SELECT
    day,
    MAX(open) as max_open,
    MAX(close) as max_close,
    MAX(low) as max_low,
    MAX(high) as max_high
FROM oclh
GROUP BY day

