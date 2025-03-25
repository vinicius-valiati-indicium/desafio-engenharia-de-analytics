with
    source_category as (
        select *
        from {{ source('erp', 'PRODUCTCATEGORY') }}
    )

    , renomeado as (
    select
    CAST(PRODUCTCATEGORYID AS INT) AS pk_product_category
    ,CAST(NAME AS VARCHAR) AS category
    -- , CAST(ROWGUID AS VARCHAR) AS row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date

    from source_category
        )

select *
from renomeado