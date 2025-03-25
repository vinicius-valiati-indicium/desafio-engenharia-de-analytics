with person as(
    select * from {{ ref('stg_erp__person') }}
)

, employee as(
    select * from {{ ref('stg_erp__employee') }}
)

select
person.PK_PERSON
, employee.NATIONAL_ID
, person.NAME_PERSON
, person.TITLE_PERSON
, person.PERSON_TYPE
, employee.JOB_TITLE
, employee.GENDER
, employee.BIRTH_DATE
, employee.HIRE_DATE
, employee.MARITAL_STATUS
, person.EMAIL_PROMOTION
, employee.IS_SALARIED
, employee.IS_EMPLOYED
, employee.VACATION_HOURS
, employee.SICK_LEAVE_HOURS

FROM person
LEFT JOIN employee on person.pk_person = employee.fk_person
-- LEFT JOIN customer on person.pk_person = customer.fk_person