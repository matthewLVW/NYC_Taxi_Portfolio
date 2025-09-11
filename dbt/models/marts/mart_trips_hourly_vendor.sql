{{ config(schema='marts', materialized='table') }}

select
  date_trunc('hour', pickup_at)                           as hour,
  vendor_id,
  count(*)::bigint                                        as trips,
  sum(total_amount)::double                               as revenue,
  avg(trip_distance_mi)::double                           as avg_miles,
  avg(date_diff('minute', pickup_at, dropoff_at))::double as avg_duration_min
from {{ ref('fact_trips') }}
group by 1,2
