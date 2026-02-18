{{ config(materialized='table') }}

select
    row_number() over (order by rideable_type) as bike_key,
    rideable_type

from (
    select distinct
        rideable_type
    from {{ ref('stg_trips') }}
)
