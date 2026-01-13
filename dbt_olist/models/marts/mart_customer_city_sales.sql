with order_items as (
    select order_id,
        customer_id,
        price,
        freight_value, 
        total_item_value as total_value
    from {{ ref('fct_order_items') }} 
    where order_status = 'delivered'
),
customer as (
    select customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('dim_customers') }} 
)
select c.customer_id,
    c.customer_city,
    c.customer_state,
    count(distinct i.order_id) as total_orders,
    sum(i.price) as total_revenue,
    sum(i.freight_value) as total_freight_value,
    sum(i.total_value) as total_sales_value
from customer c
join order_items i
    using (customer_id)
group by 
    c.customer_id,
    c.customer_city,
    c.customer_state
