with
    source_sales_reason as (
        select *
        from {{ source('erp', 'SALESREASON') }}
    )

    , renomeado as (
    select 
    cast(SALESREASONID as INT) AS pk_sales_reason
    , cast(NAME as VARCHAR) AS reason
    , cast(REASONTYPE as VARCHAR) AS reason_type
    -- , cast(MODIFIEDDATE as TIMESTAMP) AS modified_date

    from source_sales_reason
        )

select *
from renomeado