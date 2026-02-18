{{ config(materialized='view') }}

select
    ride_id as trip_id,
    
    rideable_type,
    
    to_timestamp(started_at) as started_at,
    to_timestamp(ended_at) as ended_at,
    
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    
    cast(start_lat as float) as start_lat,
    cast(start_lng as float) as start_lng,
    cast(end_lat as float) as end_lat,
    cast(end_lng as float) as end_lng,
    
    member_casual as rider_type,
    
    datediff(
        minute,
        to_timestamp(started_at),
        to_timestamp(ended_at)
    ) as trip_duration

from {{ source('raw', 'raw_citibike_trips') }}
