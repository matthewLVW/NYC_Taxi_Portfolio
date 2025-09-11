{{ config(materialized='table', schema='main_gold') }}

with bounds as (
  select
    (least(
       (select min(date_day) from {{ ref('fact_trips') }}),
       (select min(date_day) from {{ ref('fact_revenue_adjustments') }})
     ) - interval 2 day) as dmin,
    (greatest(
       (select max(date_day) from {{ ref('fact_trips') }}),
       (select max(date_day) from {{ ref('fact_revenue_adjustments') }})
     ) + interval 2 day) as dmax
),
series as (
  -- DuckDB supports generate_series for dates
  select gs::date as date_day
  from bounds, generate_series(dmin, dmax, interval 1 day) as t(gs)
)
select
  date_day,
  extract(year    from date_day)::int    as year,
  extract(quarter from date_day)::int    as quarter,
  extract(month   from date_day)::int    as month,
  extract(day     from date_day)::int    as day,
  strftime(date_day, '%Y-%m')            as year_month,
  strftime(date_day, '%A')               as day_name,
  extract(isodow  from date_day)::int    as iso_dow,
  (extract(isodow from date_day) in (6,7)) as is_weekend
from series
order by 1
