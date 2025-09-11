{{ config(materialized='table', schema='gold') }}

with t as (
  select
    -- derive the grain for the date dimension
    date(pickup_at) as date_day,
    *
  from {{ ref('stg_trips') }}
)
select
  -- grain: one row per staged trip (using dup_key as a stable key)
  t.dup_key                               as trip_key,

  -- foreign keys to dims
  t.date_day                               as date_day,
  t.vendor_id                              as vendor_id,
  t.payment_type                           as payment_type,
  t.pu_location_id                         as pu_location_id,
  t.do_location_id                         as do_location_id,

  -- measures and attributes
  t.pickup_at,
  t.dropoff_at,
  t.passenger_count,
  t.trip_distance_mi,
  t.rate_code_id,
  t.store_and_fwd_flag,
  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.congestion_surcharge,
  t.airport_fee,
  t.cbd_congestion_fee,
  t.total_amount,
  t.manual_total,
  t.duration_min,
  t.speed_mph,
  t.qa_in_file_window,
  t.qa_outlier_distance,
  t.qa_outlier_speed,
  t.qa_is_fare_mismatch,
  t.qa_is_adjustment,
  t.source_file
from t
