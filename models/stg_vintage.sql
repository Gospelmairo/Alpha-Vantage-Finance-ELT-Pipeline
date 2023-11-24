WITH source AS (
    SELECT * FROM {{ source('Alt_engin', 'vintage_data_table') }}
),
renamed AS (
    SELECT
        volume,
        high,
        close,
        time_stamp,
        symbol,
        "output size",
        "time zone",
        open,
        low,
        "last refreshed",
        interval
    FROM source
)
SELECT * FROM renamed;








-- WITH source as (

--     SELECT * FROM {{ source('Alt_engin', 'vintage_data_table') }}

-- ),

-- renamed AS (

--     SELECT
--         volume,
--         high,
--         close,
--         time_stamp,
--         symbol,
--         output size,
--         time zone,
--         open,
--         low,
--         last refreshed,
--         interval
--     FROM source

-- )

-- SELECT * FROM renamed;