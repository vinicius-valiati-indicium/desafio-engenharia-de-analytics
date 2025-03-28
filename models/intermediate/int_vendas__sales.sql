with sales_details as (
    select * from {{ ref('stg_erp__sales_order_details') }}
)

, sales_reason as (
    select * from {{ ref('stg_erp__sales_reason') }}
)

, sales as (
    select * from {{ ref('stg_erp__sales_order') }}
)

, sales_order_reason as (
    select * from {{ ref('stg_erp__sales_order_reason') }}
)

, currency as (
    select * from {{ ref('stg_erp__currency_rate') }}
)

, credit_card as(
    select * from {{ ref('stg_erp__credit_card') }}
)

, joined as (
    select
    sales_details.pk_sales_order_details
    , sales_details.fk_product
    , sales_details.fk_sales_order
    , sales_details.quantity
    , sales_details.unit_price
    , sales_details.unit_price_discount

    , sales.fk_customer
    , sales.fk_employee
    , sales.fk_territory
    , sales.fk_bill_address
    , sales.fk_ship_address  
    , sales.fk_ship_method  
    , sales.fk_credit_card  
    , sales.fk_currency_rate  
    , sales.revision_number  
    , sales.order_date  
    , sales.due_date  
    , sales.ship_date  
    , sales.order_status
    , ((sales.taxamt * sales_details.quantity * sales_details.unit_price) / sum(sales_details.quantity * sales_details.unit_price) OVER (PARTITION BY sales_details.fk_sales_order)) AS taxamt
    , ((sales.freight * sales_details.quantity * sales_details.unit_price) / sum(sales_details.quantity * sales_details.unit_price) OVER (PARTITION BY sales_details.fk_sales_order)) AS freight
    , sales.is_online  
    , sales.order_number  
    , sales.account_number  
    , sales.approval_code_credit_card  
    , sales.comment

    , currency.from_currency
    , currency.to_currency
    , currency.avg_rate
    , currency.end_rate

    , credit_card.card_type

    from sales_details
    left join sales on sales_details.fk_sales_order = sales.pk_sales_order
    left join currency on sales.fk_currency_rate = currency.pk_currency_rate and sales.order_date = currency.currency_date
    left join credit_card on sales.fk_credit_card = credit_card.pk_creditcard
)

, metrics as (
    select
    pk_sales_order_details
    , fk_sales_order
    , fk_product
    , fk_customer
    , fk_employee
    , fk_territory
    , fk_bill_address
    , fk_ship_address  
    , fk_ship_method  
    , fk_credit_card

    , order_date
    , ship_date  
    , due_date  

    , quantity
    , quantity * unit_price AS price
    , unit_price_discount
    , (quantity * unit_price) * (1 - unit_price_discount) as net_price

    , from_currency
    , avg_rate
    , to_currency

    , CASE WHEN avg_rate is null 
    then round((quantity * unit_price) , 2)
    else round((quantity * unit_price) * avg_rate, 2)
    end as price_converted

    , CASE WHEN avg_rate is null 
    then round((quantity * unit_price) * (1 - unit_price_discount) , 2)
    else round(((quantity * unit_price) * (1 - unit_price_discount)) * avg_rate, 2)
    end as net_price_converted
    
    ,taxamt
    ,freight
    , CASE 
        WHEN order_status = 1 THEN 'In process'
        WHEN order_status = 2 THEN 'Approved'
        WHEN order_status = 3 THEN 'Backordered'
        WHEN order_status = 4 THEN 'Rejected'
        WHEN order_status = 5 THEN 'Shipped'
        WHEN order_status = 6 THEN 'Cancelled'
        ELSE 'Others'
        END AS order_status
    , is_online
    , card_type  
    , CASE WHEN comment is null then 'No comment' else comment end as comment
    from joined
)

select *
from metrics

