{{ config(materialized='table') }}

with dates as (

    select distinct
        date(started_at) as trip_date
    from {{ ref('stg_trips') }}

)

select
    row_number() over (order by trip_date) as date_key,
    trip_date,
    year(trip_date) as year,
    month(trip_date) as month,
    day(trip_date) as day,
    dayofweek(trip_date) as day_of_week,
    to_char(trip_date, 'Day') as day_name,
    to_char(trip_date, 'Month') as month_name
from dates
