{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('stg_trips') }}

)

select
    base.trip_id,
    
    dr.rider_key,
    db.bike_key,
    ds_start.station_key as start_station_key,
    ds_end.station_key as end_station_key,
    dd.date_key,
    
    base.trip_duration,
    1 as ride_count

from base

left join {{ ref('dim_rider') }} dr
    on base.rider_type = dr.rider_type

left join {{ ref('dim_bike') }} db
    on base.rideable_type = db.rideable_type

left join {{ ref('dim_station') }} ds_start
    on base.start_station_id = ds_start.station_id

left join {{ ref('dim_station') }} ds_end
    on base.end_station_id = ds_end.station_id

left join {{ ref('dim_date') }} dd
    on date(base.started_at) = dd.trip_date
