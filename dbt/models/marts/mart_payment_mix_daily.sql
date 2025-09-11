{{ config(schema='marts', materialized='table') }}

with d as (
  select
    date_day,
    payment_type,
    count(*)::bigint          as trips,
    sum(total_amount)::double as revenue
  from {{ ref('fact_trips') }}
  group by 1,2
)
select
  d.date_day,
  d.payment_type,
  d.trips,
  d.revenue,
  d.revenue / nullif(d.trips, 0)                     as avg_ticket,
  d.trips::double / nullif(sum(d.trips) over (partition by d.date_day), 0) as share
from d
