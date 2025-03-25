with
    source_sales_order_reason as (
        select *
        from {{ source('erp', 'SALESORDERHEADERSALESREASON') }}
    )

    , renomeado as (
    select 
    cast(SALESORDERID as int) as fk_sales_order
    , cast(SALESREASONID as int) as fk_sales_reason
    from source_sales_order_reason
        )

select *
from renomeado