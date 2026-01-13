with orders as (
    select *
    from {{ ref('stg_orders') }} o
),
order_items as (
    select *
    from {{ ref('stg_order_items') }}
)
select * from orders 
join order_items 
    using (order_id)