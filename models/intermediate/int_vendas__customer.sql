with customer as (
    select * from {{ ref('stg_erp__customer') }}
)

, person as(
    select * from {{ ref('stg_erp__person') }}
)

, store as(
    select * from {{ ref('stg_erp__store') }}
)

select 
customer.pk_customer
, person.name_person
, person.title_person
, person.person_type
, store.store_name
from customer
left join person on person.pk_person = customer.fk_person
left join store on store.pk_store = customer.fk_store