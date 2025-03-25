with state_province as (
    select * from {{ ref('stg_erp__state_province') }}
)

, country as (
    select * from {{ ref('stg_erp__country') }}
)

, address as (
    select * from {{ ref('stg_erp__address') }}
)

, joined as (
    select
    address.PK_ADDRESS
    , state_province.FK_TERRITORY
    , country.country_name
    , state_province.COUNTRY_CODE
    , state_province.STATE_NAME
    , state_province.STATE_CODE
    , address.CITY
    , address.ADDRESS_LINE_1
    , address.ADDRESS_LINE_2
    , address.POSTAL_CODE
    , state_province.IS_ONLY_STATE_PROVINCE_FLAG
    from address
    left join state_province on state_province.pk_state_province = address.fk_state_province
    left join country on country.country_code = state_province.country_code
)

select * from joined