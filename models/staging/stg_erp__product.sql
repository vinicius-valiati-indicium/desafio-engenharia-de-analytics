with
    source_product as (
        select *
        from {{ source('erp', 'PRODUCT') }}
    )

    , renomeado as (
    select
    CAST(PRODUCTID AS INT) AS pk_product
    ,CAST(NAME AS VARCHAR) AS product_name 
    ,CAST(PRODUCTNUMBER AS VARCHAR) AS product_code
    ,CAST(MAKEFLAG AS VARCHAR) AS is_manufactured_in_house
    ,CAST(FINISHEDGOODSFLAG AS VARCHAR) AS is_salable
    ,CAST(COLOR AS VARCHAR) AS product_color
    ,CAST(SAFETYSTOCKLEVEL AS INT) AS safety_stock_level
    ,CAST(REORDERPOINT AS INT) AS reorder_point
    ,CAST(STANDARDCOST AS numeric(18,2)) AS standart_cost
    ,CAST(LISTPRICE AS numeric(18,2)) AS list_price
    ,CAST(SIZE AS VARCHAR) AS size
    ,CAST(SIZEUNITMEASURECODE AS VARCHAR) AS size_unit_measure
    ,CAST(WEIGHT AS numeric(18,2)) AS weight
    ,CAST(WEIGHTUNITMEASURECODE AS VARCHAR) AS weith_unit_measure
    ,CAST(DAYSTOMANUFACTURE AS INT) AS days_to_manufacture
    ,CASE 
        WHEN PRODUCTLINE = 'R' THEN 'Road'
        WHEN PRODUCTLINE = 'M' THEN 'Mountain'
        WHEN PRODUCTLINE = 'T' THEN 'Touring'
        WHEN PRODUCTLINE = 'S' THEN 'Standart'
        ELSE 'Others'
     END AS product_line
    ,CASE
        WHEN CLASS = 'H' THEN 'High'
        WHEN CLASS = 'M' THEN 'Medium'
        WHEN CLASS = 'L' THEN 'Low'
        ELSE 'Others'
     END AS CLASS
    ,CASE 
        WHEN STYLE = 'W' THEN 'Womens'
        WHEN STYLE = 'M' THEN 'Mens'
        WHEN STYLE = 'U' THEN 'Universal'
        ELSE 'Others'
     END AS STYLE
    ,CAST(PRODUCTSUBCATEGORYID AS INT) AS fk_product_sub_category
    ,CAST(PRODUCTMODELID AS INT) AS fk_product_model_id
    ,CAST(SELLSTARTDATE AS DATE) AS sell_start_date
    ,CAST(SELLENDDATE AS DATE) AS sell_end_date
    
    -- ,CAST(DISCONTINUEDDATE AS DATE) AS discontinued_date
    -- , CAST(ROWGUID AS VARCHAR) AS row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    from source_product
        )

select *
from renomeado