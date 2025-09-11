{{ config(materialized='table', schema='main_gold') }}

with admin as (
  select *
  from {{ source('silver', 'trips_admin') }}
)
select
  -- derive a valid date
  coalesce(cast(pickup_at as date), cast(dropoff_at as date)) as date_day,

  -- coerce out-of-range/unknown to 5='Unknown' and cast to smallint
  cast(
    case
      when payment_type between 1 and 6 then payment_type
      else 5
    end as smallint
  ) as payment_type,

  pu_location_id,
  do_location_id,

  total_amount,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  airport_fee,
  cbd_congestion_fee,

  true as is_adjustment,
  source_year,
  source_month,
  source_file
from admin
where coalesce(cast(pickup_at as date), cast(dropoff_at as date)) is not null
