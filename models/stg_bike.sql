with
    bike as (
        select
            ride_id,
            replace(started_at, '"', '') as started_at,
            replace(ended_at, '"', '') as ended_at,
            start_station_name,
            start_statio_id as start_station_id,
            end_station_name,
            end_station_id,
            start_lat,
            start_lng,
            end_lat,
            end_lng,
            member_csual
        from {{ source("DEMO", "bike2") }}
        where ride_id != 'ride_id'
        limit 100
    )
select *
from bike
