with
    source_sales_order_detail as (
        select *
        from {{ source('erp', 'SALESORDERDETAIL') }}
    )

    , renomeado as (
    select 
    CAST(SALESORDERDETAILID AS INT) AS pk_sales_order_details
    ,CAST(SALESORDERID AS INT) AS fk_sales_order
    ,CAST(PRODUCTID AS INT) AS fk_product
    ,CAST(SPECIALOFFERID AS INT) AS fk_special_offer
    ,CAST(CARRIERTRACKINGNUMBER AS VARCHAR) AS tracking_number
    ,CAST(ORDERQTY AS INT) AS quantity
    ,CAST(UNITPRICE AS numeric(18,4)) AS unit_price
    ,CAST(UNITPRICEDISCOUNT AS numeric(18,4)) AS unit_price_discount

    -- , CAST(ROWGUID AS VARCHAR) AS  row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date

    from source_sales_order_detail
        )

select *
from renomeado