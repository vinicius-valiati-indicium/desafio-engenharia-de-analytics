with
    source_sales_order as (
        select *
        from {{ source('erp', 'SALESORDERHEADER') }}
    )

    , renomeado as (
    select 
    CAST(SALESORDERID AS INT) AS pk_sales_order
    ,CAST(CUSTOMERID AS INT) AS fk_customer
    ,CAST(SALESPERSONID AS INT) AS fk_employee
    ,CAST(TERRITORYID AS INT) AS fk_territory
    ,CAST(BILLTOADDRESSID AS INT) AS fk_bill_address
    ,CAST(SHIPTOADDRESSID AS INT) AS fk_ship_address
    ,CAST(SHIPMETHODID AS INT) AS fk_ship_method
    ,CAST(CREDITCARDID AS INT) AS fk_credit_card
    ,CAST(CURRENCYRATEID AS INT) AS fk_currency_rate
    ,CAST(REVISIONNUMBER AS INT) AS revision_number
    ,CAST(ORDERDATE AS DATE) AS order_date
    ,CAST(DUEDATE AS DATE) AS due_date
    ,CAST(SHIPDATE AS DATE) AS ship_date
    ,CAST(STATUS AS INT) AS order_status
    ,CAST(SUBTOTAL AS numeric(18,2)) AS sub_total
    ,CAST(TAXAMT AS numeric(18,2)) AS taxamt
    ,CAST(FREIGHT AS numeric(18,2)) AS freight
    ,CAST(TOTALDUE AS numeric(18,2)) AS total
    ,CAST(ONLINEORDERFLAG AS VARCHAR) AS is_online
    ,CAST(PURCHASEORDERNUMBER AS VARCHAR) AS order_number
    ,CAST(ACCOUNTNUMBER AS VARCHAR) AS account_number
    ,CAST(CREDITCARDAPPROVALCODE AS VARCHAR) AS approval_code_credit_card
    ,CAST(COMMENT AS VARCHAR) AS comment
    
    -- , CAST(ROWGUID AS VARCHAR) AS  row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date

    from source_sales_order
        )

select *
from renomeado