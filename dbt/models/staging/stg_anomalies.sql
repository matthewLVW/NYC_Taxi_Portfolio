{{ config(materialized='view') }}

select
    *,
    date_trunc('day', pickup_at)::date as pickup_date
from {{ source('silver','trips_anomalies') }}
