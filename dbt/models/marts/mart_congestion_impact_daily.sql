{{ config(materialized='table', schema='marts') }}

with base as (
  select
    date_day,
    case when congestion_surcharge > 0 then 1 else 0 end as has_congestion,
    total_amount,
    fare_amount
  from {{ ref('fact_trips') }}
)
select
  date_day,
  sum(has_congestion)::bigint as trips_with_congestion,
  count(*)::bigint           as trips_total,
  sum(case when has_congestion=1 then total_amount else 0 end)::double as revenue_with_congestion,
  sum(case when has_congestion=0 then total_amount else 0 end)::double as revenue_without_congestion,
  avg(case when has_congestion=1 then fare_amount end)::double        as avg_fare_with_congestion,
  avg(case when has_congestion=0 then fare_amount end)::double        as avg_fare_without_congestion
from base
group by 1
