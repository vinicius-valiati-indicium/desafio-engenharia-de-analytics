with
    source_person as (
        select *
        from {{ source('erp', 'PERSON') }}
    )

    , renomeado as (
    select 
     cast(BUSINESSENTITYID as INT) as pk_person
    , cast(TITLE as VARCHAR) as title_person
    , COALESCE(FIRSTNAME, '') || ' ' || COALESCE(MIDDLENAME, '') || ' ' || COALESCE(LASTNAME, '') || ' ' || COALESCE(SUFFIX, '') as name_person
    , cast(EMAILPROMOTION as INT) as email_promotion
    , CASE 
        WHEN PERSONTYPE = 'SC' THEN 'Store contact'
        WHEN PERSONTYPE = 'IN' THEN 'Individual Customer' 
        WHEN PERSONTYPE = 'SP' THEN 'Sales Person'
        WHEN PERSONTYPE = 'EM' THEN 'Employee (non-sales)'
        WHEN PERSONTYPE = 'VC' THEN 'Vendor Contact'
        WHEN PERSONTYPE = 'GC' THEN 'General contact'
        ELSE 'Others'    
     end as person_type

    -- don't have any importante informartion 
    -- , cast(ROWGUID as VARCHAR) as row_guid
    -- , cast(MODIFIEDDATE as TIMESTAMP) as modified_date
    -- , cast(ADDITIONALCONTACTINFO as VARCHAR) as additional_contact_info
    -- , cast(DEMOGRAPHICS as VARCHAR) as demographics

    -- has only one value
    -- , cast(NAMESTYLE as ) as name_style

    from source_person
        )

select *
from renomeado