with
    source_address as (
        select *
        from {{ source('erp', 'ADDRESS') }}
    )

    , renomeado as (
    select 
    CAST(ADDRESSID AS INT) AS pk_address
    , CAST(STATEPROVINCEID AS INT) AS fk_state_province
    , CAST(ADDRESSLINE1 AS VARCHAR) AS address_line_1
    , CAST(ADDRESSLINE2 AS VARCHAR) AS address_line_2
    , CAST(CITY AS VARCHAR) AS city
    , CAST(POSTALCODE AS VARCHAR) AS postal_code
    
    -- , CAST(SPATIALLOCATION AS VARCHAR) AS spatiall_location
    -- , CAST(ROWGUID AS VARCHAR) AS  row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    from source_address
        )

select *
from renomeado