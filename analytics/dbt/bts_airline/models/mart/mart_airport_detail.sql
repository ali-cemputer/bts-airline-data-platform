{{ config(
    materialized='table'
) }}

with flight_data as (
    select * from {{ ref('stg_carrier_report') }}
),

-- 1. Calculate metrics for flights departing from the airport
departures as (
    select
        origin as airport_code,
        origin_city_name as city_name,
        count(*) as total_departures,
        round(avg(taxi_out), 2) as avg_taxi_out_mins,
        round(avg(dep_delay), 2) as avg_dep_delay_mins,
        sum(cast(diverted as int64)) as total_diverted
    from flight_data
    group by 1, 2
),

-- 2. Calculate metrics for flights arriving at the airport
arrivals as (
    select
        dest as airport_code,
        count(*) as total_arrivals,
        round(avg(taxi_in), 2) as avg_taxi_in_mins,
        round(avg(arr_delay), 2) as avg_arr_delay_mins
    from flight_data
    group by 1
),

-- 3. Safely find the top carrier (airline with the most departures) for each airport
carrier_counts as (
    select 
        origin as airport_code,
        reporting_airline,
        count(*) as flight_count
    from flight_data
    group by 1, 2
),

top_carriers as (
    select 
        airport_code,
        reporting_airline as top_carrier
    from carrier_counts
    -- QUALIFY filters the window function result directly, taking the #1 carrier per airport
    qualify row_number() over (partition by airport_code order by flight_count desc) = 1
),

-- 4. Bring everything together (FULL OUTER JOIN ensures no airport is left behind)
airport_detail as (
    select
        coalesce(d.airport_code, a.airport_code) as airport_code,
        d.city_name,
        coalesce(d.total_departures, 0) as total_departures,
        coalesce(a.total_arrivals, 0) as total_arrivals,
        (coalesce(d.total_departures, 0) + coalesce(a.total_arrivals, 0)) as total_traffic,
        d.avg_taxi_out_mins,
        a.avg_taxi_in_mins,
        d.avg_dep_delay_mins,
        a.avg_arr_delay_mins,
        coalesce(d.total_diverted, 0) as total_diverted,
        tc.top_carrier
        
    from departures d
    full outer join arrivals a on d.airport_code = a.airport_code
    left join top_carriers tc on coalesce(d.airport_code, a.airport_code) = tc.airport_code
)

select * from airport_detail
-- Order by the busiest airport
order by total_traffic desc