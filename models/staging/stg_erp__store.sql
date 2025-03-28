with
    source_store as (
        select *
        from {{ source('erp', 'STORE') }}
    )

    , renomeado as (
    select
    CAST(BUSINESSENTITYID AS INT) AS pk_store
    , CAST(NAME AS VARCHAR) AS store_name
    , CAST(SALESPERSONID AS INT) AS fk_customer
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    FROM source_store)
        
select *
from renomeado