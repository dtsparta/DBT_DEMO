WITH bike AS (
    SELECT
        RIDE_ID,
        REPLACE(STARTED_AT, '"', '') AS STARTED_AT,
        REPLACE(ENDED_AT, '"', '') AS ENDED_AT,
        START_STATION_NAME,
        START_STATIO_ID AS START_STATION_ID,
        END_STATION_NAME,
        END_STATION_ID,
        START_LAT,
        START_LNG,
        END_LAT,
        END_LNG,
        MEMBER_CSUAL
    FROM {{ source('DEMO', 'bike2') }}
    WHERE ride_id != 'ride_id'
    LIMIT 100
)
SELECT * FROM bike