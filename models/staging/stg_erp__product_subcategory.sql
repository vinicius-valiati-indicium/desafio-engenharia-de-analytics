with
    source_subcategory as (
        select *
        from {{ source('erp', 'PRODUCTSUBCATEGORY') }}
    )

    , renomeado as (
    select
    CAST(PRODUCTSUBCATEGORYID AS INT) AS pk_product_subcategory
    ,CAST(PRODUCTCATEGORYID AS INT) AS fk_product_category
    ,CAST(NAME AS VARCHAR) AS subcategory
    -- , CAST(ROWGUID AS VARCHAR) AS row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date

    from source_subcategory
        )

select *
from renomeado