select 
    order_id,
    order_purchase_date,
    purchase_hour,
    sum(price) as total_sales,
    sum(freight_value) as total_freight,
    sum(price + freight_value) as total_order_value
from {{ ref('fct_order_items') }}
group by order_id, order_purchase_date, purchase_hour