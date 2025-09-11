{{ config(materialized='table', schema='gold') }}

with ids as (
  select distinct pu_location_id as location_id
  from {{ ref('stg_trips') }} where pu_location_id is not null
  union
  select distinct do_location_id as location_id
  from {{ ref('stg_trips') }} where do_location_id is not null
),
zones as (
  select
    cast("LocationID" as int) as location_id,
    "Borough" as borough,
    "Zone" as zone,
    service_zone
  from {{ ref('taxi_zones') }}
)
select
  i.location_id,
  coalesce(z.borough, 'Unknown')      as borough,
  coalesce(z.zone, 'Unknown')         as zone,
  coalesce(z.service_zone, 'Unknown') as service_zone
from ids i
left join zones z using (location_id)
