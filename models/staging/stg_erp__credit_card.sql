with
    source_credit_card as (
        select *
        from {{ source('erp', 'CREDITCARD') }}
    )

    , renomeado as (
    select
    CAST(CREDITCARDID AS INT) AS pk_creditcard
    ,CAST(CARDTYPE AS VARCHAR) AS card_type
    ,CAST(CARDNUMBER AS INT) AS card_number
    ,CAST(EXPMONTH AS INT) AS exp_month
    ,CAST(EXPYEAR AS INT) AS exp_year
    
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    FROM source_credit_card)
        

select *
from renomeado