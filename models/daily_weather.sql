WITH daily_weather AS (
    SELECT 
        date(time) as daily_weather,
        weather,
        temp,
        pressure,
        humidity,
        clouds
      FROM {{ source('DEMO', 'weather') }}
      LIMIT 10
),
daily_weather_agg AS (
    select 
        daily_weather,
        weather,
        count(weather)
     from daily_weather
    group by daily_weather, weather
)
SELECT *
FROM daily_weather_agg