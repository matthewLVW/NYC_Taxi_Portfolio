{{ config(schema='marts', materialized='table') }}

with x as (
  select
    dd.year_month,
    t.vendor_id,
    count(*)::bigint                                        as trips,
    sum(total_amount)::double                               as revenue,
    avg(trip_distance_mi)::double                           as avg_miles,
    avg(date_diff('minute', pickup_at, dropoff_at))::double as avg_duration_min,
    sum(tip_amount)::double                                 as tips
  from {{ ref('fact_trips') }} t
  join {{ ref('dim_date') }} dd on dd.date_day = t.date_day
  group by 1,2
),
tot as (
  select year_month, sum(revenue)::double as month_revenue
  from x group by 1
)
select
  x.year_month,
  x.vendor_id,
  x.trips,
  x.revenue,
  x.revenue / nullif(x.trips,0) as avg_ticket,
  x.avg_miles,
  x.avg_duration_min,
  x.tips / nullif(x.revenue,0)  as tip_rate,
  x.revenue / nullif(tot.month_revenue,0) as revenue_share
from x
join tot using (year_month)
