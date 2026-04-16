{{ config(
    materialized='table'
) }}

with delayed_flights as (
    select * from {{ ref('stg_carrier_report') }}
    -- We only focus on flights that have genuinely experienced delays in arrival.
    -- Not shouldn't disrupt this analyses cancelled or coming on time
    where arr_delay > 0
),

aggregated_delays as (
    select
        reporting_airline,
        count(*) as total_delayed_flights,
        
        -- sum of delay mins
        sum(arr_delay) as total_delay_minutes,
        sum(carrier_delay) as total_carrier_delay,
        sum(weather_delay) as total_weather_delay,
        sum(nas_delay) as total_nas_delay,
        sum(security_delay) as total_security_delay,
        sum(late_aircraft_delay) as total_late_aircraft_delay
        
    from delayed_flights
    group by reporting_airline
)

select
    reporting_airline,
    total_delayed_flights,
    total_delay_minutes,
    
    -- We are calculating the "Percentage Distribution" for Streamlit Pie Charts.
    round(safe_divide(total_carrier_delay, total_delay_minutes) * 100, 2) as carrier_delay_pct,
    round(safe_divide(total_weather_delay, total_delay_minutes) * 100, 2) as weather_delay_pct,
    round(safe_divide(total_nas_delay, total_delay_minutes) * 100, 2) as nas_delay_pct,
    round(safe_divide(total_security_delay, total_delay_minutes) * 100, 2) as security_delay_pct,
    round(safe_divide(total_late_aircraft_delay, total_delay_minutes) * 100, 2) as late_aircraft_delay_pct

from aggregated_delays
-- most delay airline come on top
order by total_delayed_flights desc