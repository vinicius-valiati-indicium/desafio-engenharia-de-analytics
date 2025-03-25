with
    source_customer as (
        select *
        from {{ source('erp', 'CUSTOMER') }}
    )

    , renomeado as (
        select
         cast(CUSTOMERID AS INT) as pk_customer
         , cast(PERSONID AS INT) as fk_person
         , cast(STOREID AS INT) as fk_store
         , cast(TERRITORYID AS INT) as fk_territory
        --  , cast(ROWGUID AS VARCHAR) as rowguid
        --  , cast(MODIFIEDDATE AS TIMESTAMP) as modified_date
        from source_customer
    )

select *
from renomeado