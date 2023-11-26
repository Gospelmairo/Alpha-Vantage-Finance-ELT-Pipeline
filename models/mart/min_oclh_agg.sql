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
    MIN(open) as min_open,
    MIN(close) as min_close,
    MIN(low) as min_low,
    MIN(high) as min_high
FROM oclh
GROUP BY day

