{{ config(
    materialized='table',
    unique_key='time_stamp'
) }}

SELECT
    time_stamp,
    open,
    close,
    low,
    high,
    volume,
    symbol
FROM {{ ref("stg_vintage") }}
