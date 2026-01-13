with raw_orders as (
    select order_id,
    customer_id,
    order_status,
    date_trunc('hour', order_purchase_timestamp) as order_purchase_timestamp
from {{ref('stg_orders')}}
)
select *, 
    extract(hour from order_purchase_timestamp) as purchase_hour
from raw_orders
