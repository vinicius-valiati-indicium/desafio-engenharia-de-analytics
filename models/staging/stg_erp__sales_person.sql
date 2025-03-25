with
    source_sales_person as (
        select *
        from {{ source('erp', 'SALESPERSON') }}
    )

    , renomeado as (
    select 
    CAST(BUSINESSENTITYID AS INT) AS fk_person
    , CAST(TERRITORYID AS INT) AS fk_territory
    , CAST(SALESQUOTA AS numeric(18,2)) AS sales_quota
    , CAST(BONUS AS numeric(18,2)) AS bonus
    , CAST(COMMISSIONPCT AS numeric(18,4)) AS comission_percent
    , CAST(SALESYTD AS numeric(18,2)) AS sales_ytd
    , CAST(SALESLASTYEAR AS numeric(18,2)) AS sales_last_year
    -- , CAST(ROWGUID AS VARCHAR) AS row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    from source_sales_person
        )

select *
from renomeado