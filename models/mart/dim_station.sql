{{ config(materialized='table') }}

with stations as (

    select
        start_station_id as station_id,
        start_station_name as station_name,
        start_lat as lat,
        start_lng as lng
    from {{ ref('stg_trips') }}

    union

    select
        end_station_id as station_id,
        end_station_name as station_name,
        end_lat as lat,
        end_lng as lng
    from {{ ref('stg_trips') }}

),

deduplicated as (

    select distinct
        station_id,
        station_name,
        lat,
        lng
    from stations
)

select
    row_number() over (order by station_id) as station_key,
    station_id,
    station_name,
    lat,
    lng
from deduplicated
