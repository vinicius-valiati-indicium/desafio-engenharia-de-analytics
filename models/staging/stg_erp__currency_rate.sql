with
    source_currency_rate as (
        select *
        from {{ source('erp', 'CURRENCYRATE') }}
    )

    , renomeado as (
    select
    CAST(CURRENCYRATEID AS INT) AS pk_currency_rate
    ,CAST(CURRENCYRATEDATE AS DATE) AS currency_date
    ,CAST(FROMCURRENCYCODE AS VARCHAR) AS from_currency
    ,CAST(TOCURRENCYCODE AS VARCHAR) AS to_currency
    ,CAST(AVERAGERATE AS numeric(18,2)) AS avg_rate
    ,CAST(ENDOFDAYRATE AS numeric(18,2)) AS end_rate
    
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    FROM source_currency_rate)
        

select *
from renomeado