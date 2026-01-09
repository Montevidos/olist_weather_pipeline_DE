with orders as (
    select
        order_id, 
        customer_id, 
        order_status,  
        order_purchase_timestamp,
        date_trunc('hour', order_purchase_timestamp) as order_timestamp_rounded,
        order_approved_at as order_approved_timestamp,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ref('stg_orders')}} o
    ),
customers as (
    select customer_id,
           customer_unique_id,
           customer_zip_code_prefix,
           customer_city,
           customer_state
    from {{ref('stg_customers')}} c
    where customer_city = 'rio de janeiro'
      and customer_state = 'RJ'
    ),
weather as (
    select TIMESTAMP,
        temperature,
        humidity,
        precipitation,
        surface_pressure,
        wind_speed
    from {{ref('stg_weather_rio')}} w
)
select o.order_id, 
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_timestamp_rounded,
    o.order_approved_timestamp,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    w.timestamp as weather_timestamp,
    w.temperature,
    w.humidity,
    w.precipitation,
    w.surface_pressure,
    w.wind_speed
from orders o
join customers c
using (customer_id)
left join weather w
on o.order_timestamp_rounded = w.timestamp

