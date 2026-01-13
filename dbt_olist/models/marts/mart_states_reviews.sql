with order_items as (
    select * from {{ ref('fct_order_items') }} i
),
reviews as (
    select review_id,
        order_id,
        review_score,
        review_creation_date
    from {{ ref('fct_reviews') }} r
),
customer as (
    select customer_id,
        customer_zip_code_prefix,
        customer_state,
        customer_city
    from {{ ref('dim_customers') }} c
)
select 
    r.review_id,
    r.order_id,
    r.review_score,
    i.customer_id,
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix
from order_items i
left join reviews r
    using (order_id)
left join customer c
    using (customer_id)

