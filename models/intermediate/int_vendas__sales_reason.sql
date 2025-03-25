with sales_reason as (
    select * from {{ ref('stg_erp__sales_reason') }}
)

, sales as (
    select * from {{ ref('stg_erp__sales_order') }}
)

, sales_order_reason as (
    select * from {{ ref('stg_erp__sales_order_reason') }}
)


, joined as (
    select
    sales.pk_sales_order
    , sales_reason.reason
    , sales_reason.reason_type
    from sales
    left join sales_order_reason on sales.pk_sales_order = sales_order_reason.fk_sales_order
    left join sales_reason on sales_reason.pk_sales_reason = sales_order_reason.fk_sales_reason
)

, metrics as (
    select
    pk_sales_order
    , CASE when reason is null then 'No reason' else reason end as reason
    , CASE WHEN reason_type is null then 'No reason' else reason_type end as reason_type
    from joined
)

select *
from metrics


