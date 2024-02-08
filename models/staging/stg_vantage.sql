with 

renamed as (

    select
        time_stamp,
        open,
        close,
        low,
        high,
        volume,
        symbol,
        output_size,
        time_zone,
        time_interval,
        last_refreshed
    from {{ source('Alt_engin', 'vintage_data_table') }}
)

select * from renamed
