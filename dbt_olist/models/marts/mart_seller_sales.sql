with order_items as (
    select * from {{ ref('fct_order_items')}}
),
sellers as (
    select * from {{ ref('dim_sellers')}}
),
products as (
    select * from {{ ref('dim_products')}}
),
raw_data as (select s.seller_id,
    p.product_category_name,
    sum(i.price) as total_sales,
    sum(i.freight_value) as total_freight,
    count(i.order_id) as total_items_sold,
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix
from order_items i
join sellers s
    using (seller_id)
join products p
    using (product_id)
group by i.seller_id,
    p.product_category_name,
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix
)
select *, 
    total_sales +total_freight as total_revenue
from raw_data
    