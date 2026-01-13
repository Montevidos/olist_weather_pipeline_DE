select 
    geolocation_zip_code_prefix,
    avg(geolocation_lat) as avg_geolocation_lat,
    avg(geolocation_lng) as avg_geolocation_lng,
    max(geolocation_city) as geolocation_city,
    max(geolocation_state) as geolocation_state
from {{ref('stg_geolocation')}}
group by 
    geolocation_zip_code_prefix