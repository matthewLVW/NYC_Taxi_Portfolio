{{ config(schema='marts', materialized='table') }}

with d as (
  select
    dd.year_month,
    t.pu_location_id,
    t.do_location_id,
    count(*)::bigint                                        as trips,
    sum(total_amount)::double                               as revenue,
    avg(trip_distance_mi)::double                           as avg_miles,
    avg(date_diff('minute', pickup_at, dropoff_at))::double as avg_duration_min
  from {{ ref('fact_trips') }} t
  join {{ ref('dim_date') }} dd on dd.date_day = t.date_day
  group by 1,2,3
)
select * from d
