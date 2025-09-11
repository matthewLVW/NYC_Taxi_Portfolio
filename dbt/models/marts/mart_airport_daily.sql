{{ config(materialized='table', schema='marts') }}

-- Airport PU side
with pu as (
  select
    t.date_day,
    'PU' as direction,
    case
      when z.zone ilike '%JFK%' then 'JFK'
      when z.zone ilike '%LGA%' or z.zone ilike '%LaGuardia%' then 'LGA'
      when z.zone ilike '%EWR%' or z.zone ilike '%Newark%' then 'EWR'
    end as airport,
    t.total_amount,
    t.fare_amount,
    t.tip_amount,
    t.trip_distance_mi,
    t.airport_fee
  from {{ ref('fact_trips') }} t
  join {{ ref('dim_zone') }} z
    on z.location_id = t.pu_location_id
  where z.service_zone = 'Airports'
),

-- Airport DO side (CTE name changed from "do" -> "do_side" to avoid reserved word)
do_side as (
  select
    t.date_day,
    'DO' as direction,
    case
      when z.zone ilike '%JFK%' then 'JFK'
      when z.zone ilike '%LGA%' or z.zone ilike '%LaGuardia%' then 'LGA'
      when z.zone ilike '%EWR%' or z.zone ilike '%Newark%' then 'EWR'
    end as airport,
    t.total_amount,
    t.fare_amount,
    t.tip_amount,
    t.trip_distance_mi,
    t.airport_fee
  from {{ ref('fact_trips') }} t
  join {{ ref('dim_zone') }} z
    on z.location_id = t.do_location_id
  where z.service_zone = 'Airports'
),

base as (
  select * from pu
  union all
  select * from do_side
)

select
  date_day,
  airport,
  direction,                                   -- 'PU' / 'DO'
  count(*)::bigint                 as trips,
  sum(total_amount)::double        as total_revenue,
  sum(fare_amount)::double         as fare_revenue,
  sum(tip_amount)::double          as tip_revenue,
  sum(airport_fee)::double         as airport_fee_total,
  avg(trip_distance_mi)::double    as avg_miles
from base
where airport is not null
group by 1,2,3
order by 1,2,3
