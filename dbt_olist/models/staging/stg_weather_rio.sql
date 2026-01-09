select TIMESTAMP::timestamp as timestamp,
       TEMPERATURE_2M::float as temperature,
       RELATIVE_HUMIDITY_2M::float as humidity,
       PRECIPITATION::float as precipitation,
       SURFACE_PRESSURE::float as surface_pressure,
       WINDSPEED_10M::float as wind_speed
from {{source('olist', 'raw_weather_rio')}}