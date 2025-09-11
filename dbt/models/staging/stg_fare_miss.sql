{{ config(materialized='view') }}

-- Subset of Clean with fare mismatch; useful for diagnostics/marts
select
    *,
    date_trunc('day', pickup_at)::date as pickup_date
from {{ source('silver','trips_fare_miss') }}
