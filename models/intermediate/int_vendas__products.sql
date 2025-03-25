with products as(
    select *
    from {{ ref('stg_erp__product') }}
)

, subcategory as(
    select *
    from {{ ref('stg_erp__product_subcategory') }}
)

, category as(
    select *
    from {{ ref('stg_erp__product_category') }}
)

, joined as(
    select
    pk_product
    , products.product_name
    , case when subcategory.subcategory is null then 'Others' else subcategory.subcategory end as subcategory
    , case when category.category is null then 'Others' else category.category end as category
    , products.product_code
    , products.is_manufactured_in_house
    , products.is_salable
    , case when products.product_color is null then 'Others' else products.product_color end as product_color
    , products.safety_stock_level
    , products.standart_cost
    , products.list_price
    , products.size
    , products.size_unit_measure
    , products.weight
    , products.weith_unit_measure
    , products.days_to_manufacture
    , products.sell_start_date
    , case when products.sell_end_date is null then 'False' else 'True' end as is_product_disabled
    from products
    left join subcategory on subcategory.pk_product_subcategory = products.fk_product_sub_category
    left join category on category.pk_product_category = subcategory.fk_product_category

)

select * from joined
