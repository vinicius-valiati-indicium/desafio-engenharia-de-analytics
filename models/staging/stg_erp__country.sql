with
    source_country_region as (
        select *
        from {{ source('erp', 'COUNTRYREGION') }}
    )

    , renomeado as (
    select 
    CAST(COUNTRYREGIONCODE AS VARCHAR) AS country_code
    , CAST(NAME AS VARCHAR) AS country_name
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    from source_country_region
        )

select *
from renomeado