select order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as date) as order_purchase_date,
    extract(hour from order_purchase_timestamp) as purchase_hour,
    product_id,
    order_item_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    price + freight_value as total_item_value
from {{ref('prep_order_items')}}