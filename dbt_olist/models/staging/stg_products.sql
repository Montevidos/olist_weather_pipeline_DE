with name as (
    select PRODUCT_CATEGORY_NAME,
           PRODUCT_CATEGORY_NAME_ENGLISH
    from {{ source('olist', 'PRODUCT_CATEGORY_NAME_TRANSLATION') }}
),
stg as (
    select p.PRODUCT_ID,
            p.PRODUCT_CATEGORY_NAME,
           n.PRODUCT_CATEGORY_NAME_ENGLISH,
           p.PRODUCT_NAME_LENGHT::integer as PRODUCT_NAME_LENGTH,
           p.PRODUCT_DESCRIPTION_LENGHT::integer as PRODUCT_DESCRIPTION_LENGTH,
           p.PRODUCT_PHOTOS_QTY::integer as PRODUCT_PHOTOS_QTY,
           p.PRODUCT_WEIGHT_G::integer as PRODUCT_WEIGHT_G,
           p.PRODUCT_LENGTH_CM::integer as PRODUCT_LENGTH_CM,
           p.PRODUCT_HEIGHT_CM::integer as PRODUCT_HEIGHT_CM,
           p.PRODUCT_WIDTH_CM::integer as PRODUCT_WIDTH_CM
    from {{ source('olist', 'raw_products') }} p
    left join name n
    on p.PRODUCT_CATEGORY_NAME = n.PRODUCT_CATEGORY_NAME
)
select * from stg