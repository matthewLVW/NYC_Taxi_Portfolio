{{ config(materialized='view') }}

-- Admin/adjustment rows from Silver
select
    *,
    date_trunc('day', pickup_at)::date as pickup_date
from {{ source('silver','trips_admin') }}
