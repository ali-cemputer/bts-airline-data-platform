{{ config(
    materialized='table'
) }}
-- we say to BQ: this is not a view. this is table.

with staging_data as (
    select * from {{ref('stg_carrier_report')}}
),

aggregated_metrics as (
    select
        flight_year,
        flight_month,
        reporting_airline,

        -- Volume Metrics
        count(*) as total_flights,
        sum(cast(cancelled as INT64)) as total_cancelled_flights,
        round(sum(cast(cancelled as INT64)) / count(*) * 100, 2) as cancellation_rate_pct,

        -- Performance Metrics
        round(avg(dep_delay), 2) as avg_dep_delay_mins,
        round(avg(arr_delay), 2) as avg_arr_delay_mins,

        -- Distribution of Delay Categories
        sum(case when delay_category = 'No Delay' then 1 else 0 end) as total_on_time_flights,
        sum(case when delay_category = 'Minor' then 1 else 0 end) as total_minor_delayed,
        sum(case when delay_category = 'Major' then 1 else 0 end) as total_major_delayed,
        sum(case when delay_category = 'Severe' then 1 else 0 end) as total_severe_delayed

    from staging_data
    group by 
        flight_year, 
        flight_month, 
        reporting_airline
)

select * from aggregated_metrics