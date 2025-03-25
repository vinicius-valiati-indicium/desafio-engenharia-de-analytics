with
    source_employee as (
        select *
        from {{ source('erp', 'EMPLOYEE') }}
    )

    , renomeado as (
    select 
    CAST(BUSINESSENTITYID AS INT) AS fk_person
    , CAST(NATIONALIDNUMBER AS INT) AS national_id
    , CAST(JOBTITLE AS VARCHAR) AS job_title
    , CAST(BIRTHDATE AS DATE) AS birth_date
    , CAST(MARITALSTATUS AS VARCHAR) AS marital_status
    , CAST(GENDER AS VARCHAR) AS gender
    , CAST(HIREDATE AS DATE) AS hire_date
    , CAST(SALARIEDFLAG AS VARCHAR) AS is_salaried
    , CAST(CURRENTFLAG AS VARCHAR) AS is_employed
    , CAST(VACATIONHOURS AS INT) AS vacation_hours
    , CAST(SICKLEAVEHOURS AS INT) AS sick_leave_hours
    
    -- , CAST(ROWGUID AS VARCHAR) AS  row_guid
    -- , CAST(MODIFIEDDATE AS TIMESTAMP) AS modified_date
    -- , CAST(ORGANIZATIONNODE AS VARCHAR) AS organization_mode
    -- , CAST(LOGINID AS VARCHAR) AS login_id

    from source_employee
        )

select *
from renomeado