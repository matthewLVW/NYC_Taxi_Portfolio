{{ config(schema='marts', materialized='table') }}

select
  date_day,
  pu_location_id as location_id,
  sum(total_amount)::double       as total_revenue,
  sum(fare_amount)::double        as fare_revenue,
  sum(tip_amount)::double         as tip_revenue,
  sum(trip_distance_mi)::double   as total_miles,
  count(*)::bigint                as trips
from {{ ref('fact_trips') }}
group by 1,2
