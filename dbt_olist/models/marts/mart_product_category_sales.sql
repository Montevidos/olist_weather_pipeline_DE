with order_items as (
    select * from {{ ref('fct_order_items')}}
),
products as (
    select * from {{ ref('dim_products')}} p
)
select p.product_id,
    p.product_category_name,
    count(i.order_id) as items_sold,
    sum(i.price) as total_revenue,
    sum(i.freight_value) as total_freight_value,
    sum(i.total_item_value) as total_sales_value     
from order_items i
join products p
    using (product_id)
group by p.product_id, p.product_category_name