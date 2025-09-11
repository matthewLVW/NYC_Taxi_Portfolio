{{ config(materialized='table', schema='gold') }}

-- Prefer seed if available; otherwise derive distincts
with src as (
  select vendor_id from {{ ref('stg_trips') }} where vendor_id is not null
)
, distinct_ids as (
  select distinct vendor_id from src
)
, lookup as (
  {% if execute and adapter.get_relation(database=None, schema=target.schema, identifier='payment_types') %}
    -- no-op; keep structure consistent
  {% endif %}
  select vendor_id, vendor_name
  from {{ ref('vendors') }}
)
select
  d.vendor_id,
  coalesce(l.vendor_name, 'Unknown') as vendor_name
from distinct_ids d
left join lookup l using (vendor_id)
