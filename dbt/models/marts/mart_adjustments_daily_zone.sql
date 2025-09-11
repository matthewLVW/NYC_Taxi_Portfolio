{{ config(materialized='table', schema='marts') }}

select
  date_day,
  cast(
    case when payment_type between 1 and 6 then payment_type else 5 end
    as smallint
  ) as payment_type,
  pu_location_id as location_id,
  count(*)::bigint              as trips,
  sum(total_amount)::double     as total_revenue
from {{ ref('fact_revenue_adjustments') }}
where date_day is not null
group by 1,2,3
