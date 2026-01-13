select order_id, 
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    case 
        when order_delivered_carrier_date <= order_estimated_delivery_date 
        then 'on time'
        when order_delivered_carrier_date is null 
        then 'unknown'
        else 'late'
    end as delivery_status,
    order_estimated_delivery_date
from {{ref('stg_orders')}}