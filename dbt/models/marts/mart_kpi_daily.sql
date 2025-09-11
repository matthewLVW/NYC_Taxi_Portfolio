{{ config(schema='marts', materialized='table') }}

with trips as (
  select
    date_day,
    count(*)                                  as trips,
    sum(total_amount)::double                 as gross_revenue,
    sum(tip_amount)::double                   as tips,
    sum(congestion_surcharge)::double         as congestion,
    sum(tolls_amount)::double                 as tolls,
    avg(trip_distance_mi)::double             as avg_miles,
    avg(date_diff('minute', pickup_at, dropoff_at))::double as avg_duration_min
  from {{ ref('fact_trips') }}
  group by 1
),
adj as (
  select date_day, sum(total_amount)::double as adj_revenue
  from {{ ref('fact_revenue_adjustments') }}
  group by 1
)
select
  t.date_day,
  t.trips,
  t.gross_revenue,
  coalesce(t.gross_revenue,0) + coalesce(a.adj_revenue,0) as net_revenue,
  t.tips,
  case when t.gross_revenue > 0 then t.tips / t.gross_revenue end as tip_rate,
  t.congestion,
  t.tolls,
  t.avg_miles,
  t.avg_duration_min,
  case when t.avg_duration_min > 0 then 60.0 * t.avg_miles / t.avg_duration_min end as avg_speed_mph
from trips t
left join adj a using (date_day)
