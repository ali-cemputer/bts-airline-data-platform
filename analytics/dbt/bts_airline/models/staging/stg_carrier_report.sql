-- Staging model: wraps the raw source table.
-- Selects only the columns we need and gives them clear names
-- All downstream mart models read from this view, not from the raw table.

with source as (
    select * from {{source('bts_raw', 'carrier_performance')}}
),

renamed as (
    select
        --flight identity
        flight_date,
        reporting_airline,
        origin,
        origin_city_name,
        dest,
        dest_city_name,

        -- schedule
        crs_dep_time,
        crs_arr_time,

        --actual times
        dep_time,
        arr_time,
        actual_elapsed_time,
        air_time,

        --delay amounts in minutes (null if no delay)
        arr_delay,
        dep_delay,
        carrier_delay,
        weather_delay,
        nas_delay,
        security_delay,
        late_aircraft_delay,

        -- delay category (derived in Spark FAZ 3)
        delay_category,

        --cancellation / diversion
        cancelled,
        cancellation_code,
        diverted,

        -- taxi times & distance
        taxi_out,
        taxi_in,
        distance,

        -- partitioning helpers 
        year as flight_year,
        month as flight_month,
        day_of_week
    
    from source
)

select * from renamed