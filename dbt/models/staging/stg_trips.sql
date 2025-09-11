{{ config(materialized='view') }}

select
  vendor_id::smallint                    as vendor_id,
  pickup_at                              as pickup_at,
  dropoff_at                             as dropoff_at,
  date_trunc('day', pickup_at)::date     as pickup_date,
  extract(hour from pickup_at)::smallint as pickup_hour,
  passenger_count::smallint              as passenger_count,
  trip_distance_mi::double               as trip_distance_mi,
  rate_code_id::smallint                 as rate_code_id,
  store_and_fwd_flag                     as store_and_fwd_flag,
  pu_location_id::int                    as pu_location_id,
  do_location_id::int                    as do_location_id,
  cast(
    case
      when payment_type in (1,2,3,4,5,6) then payment_type
      when payment_type is null then 5
      else 5
    end
  as smallint) as payment_type,
  fare_amount::double                    as fare_amount,
  extra::double                          as extra,
  mta_tax::double                        as mta_tax,
  tip_amount::double                     as tip_amount,
  tolls_amount::double                   as tolls_amount,
  improvement_surcharge::double          as improvement_surcharge,
  total_amount::double                   as total_amount,
  congestion_surcharge::double           as congestion_surcharge,
  airport_fee::double                    as airport_fee,
  cbd_congestion_fee::double             as cbd_congestion_fee,
  manualTotal::double                    as manual_total,      -- alias for dbt/tests
  duration_min::double                   as duration_min,
  speed_mph::double                      as speed_mph,
  qa_in_file_window::boolean             as qa_in_file_window,
  qa_outlier_distance::boolean           as qa_outlier_distance,
  qa_outlier_speed::boolean              as qa_outlier_speed,
  qa_is_fare_mismatch::boolean           as qa_is_fare_mismatch,
  qa_is_adjustment::boolean              as qa_is_adjustment,
  dup_key                                as dup_key,
  qa_is_duplicate_in_file::boolean       as qa_is_duplicate_in_file,
  source_year::int                       as source_year,
  source_month::int                      as source_month,
  source_file                            as source_file
from {{ source('silver','trips_clean') }}
