{{ config(materialized='table') }}

select
    row_number() over (order by rider_type) as rider_key,
    rider_type

from (
    select distinct
        rider_type
    from {{ ref('stg_trips') }}
)
