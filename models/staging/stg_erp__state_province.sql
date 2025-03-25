with
    source_state_province as (
        select *
        from {{ source('erp', 'STATEPROVINCE') }}
    )

    , renomeado as (
    select 
     CAST(STATEPROVINCEID AS INT) AS pk_state_province
    , CAST(TERRITORYID AS INT) AS fk_territory
    , CAST(STATEPROVINCECODE AS VARCHAR) AS state_code
    , CAST(NAME AS VARCHAR) AS state_name
    , CAST(COUNTRYREGIONCODE AS VARCHAR) AS country_code
    , CAST(ISONLYSTATEPROVINCEFLAG AS VARCHAR) AS is_only_state_province_flag
    
    -- , CAST(ROWGUID AS VARCHAR) AS  row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    from source_state_province
        )

select *
from renomeado