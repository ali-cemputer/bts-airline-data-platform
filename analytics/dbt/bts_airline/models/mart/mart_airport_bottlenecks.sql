{{ config(
    materialized='table'
) }}

with staging_data as (
    select * from {{ ref('stg_carrier_report') }}
),

-- Departure airport (Origin) Metrics
departures as (
    select
        origin as airport_code,
        origin_city_name as city_name,
        count(*) as total_departures,
        -- Taxi Out
        round(avg(taxi_out), 2) as avg_taxi_out_mins,
        round(avg(dep_delay), 2) as avg_dep_delay_mins
    from staging_data
    group by origin, origin_city_name
),

-- 2. Arrived Airport (Dest) Metrics
arrivals as (
    select
        dest as airport_code,
        dest_city_name as city_name,
        count(*) as total_arrivals,
        -- Taxi In: how many minutes
        round(avg(taxi_in), 2) as avg_taxi_in_mins,
        round(avg(arr_delay), 2) as avg_arr_delay_mins
    from staging_data
    group by dest, dest_city_name
),

-- 3. Combine the two to create an "Airport Report Card".
final as (
    select
        coalesce(d.airport_code, a.airport_code) as airport_code,
        coalesce(d.city_name, a.city_name) as city_name,
        coalesce(d.total_departures, 0) as total_departures,
        coalesce(a.total_arrivals, 0) as total_arrivals,
        (coalesce(d.total_departures, 0) + coalesce(a.total_arrivals, 0)) as total_traffic,
        
        -- departure bottlenecks
        d.avg_taxi_out_mins,
        d.avg_dep_delay_mins,
        
        -- Arrive bottlenecks
        a.avg_taxi_in_mins,
        a.avg_arr_delay_mins

    from departures d
    full outer join arrivals a
        on d.airport_code = a.airport_code
)

select * from final
-- Most busiest airports on top
order by total_traffic desc