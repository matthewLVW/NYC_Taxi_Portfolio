{{ config(schema='marts', materialized='table') }}

with base as (
  select
    date_day,
    pu_location_id as location_id,
    date_diff('minute', pickup_at, dropoff_at) as dur_min,
    trip_distance_mi,
    case when date_diff('minute', pickup_at, dropoff_at) > 0
         then 60.0 * trip_distance_mi / date_diff('minute', pickup_at, dropoff_at)
         else null end as speed_mph,
    total_amount
  from {{ ref('fact_trips') }}
)
select
  date_day,
  location_id,
  count_if(trip_distance_mi > 150 or trip_distance_mi < 0)::bigint as distance_outliers,
  count_if(dur_min < 1 or dur_min > 360)::bigint                    as duration_outliers,
  count_if(speed_mph > 80)::bigint                                   as speed_outliers,
  count_if(total_amount < 0)::bigint                                 as negative_total,
  -- roll into a single headline metric too
  (count_if(trip_distance_mi > 150 or trip_distance_mi < 0)
   + count_if(dur_min < 1 or dur_min > 360)
   + count_if(speed_mph > 80)
   + count_if(total_amount < 0))::bigint                               as anomalies
from base
group by 1,2
