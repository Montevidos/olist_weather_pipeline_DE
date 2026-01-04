select * from {{ ref('stg_products') }} 
where PRODUCT_CATEGORY_NAME_ENGLISH is null