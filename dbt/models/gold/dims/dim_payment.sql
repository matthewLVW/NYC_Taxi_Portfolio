{{ config(materialized='table', schema='gold') }}

-- Build payment dimension strictly from the seed to avoid stray values
select
  cast(payment_type as smallint) as payment_type,
  payment_desc
from {{ ref('payment_types') }}
