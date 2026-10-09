{{ config(materialized='incremental', unique_key='CustomerID') }}

select
    CustomerID,
    CustomerName,
    CustomerCategory,
    BuyingGroup,
    DeliveryCityID,
    CreditLimit,
    case
        when CreditLimit >= 40000 then 'Enterprise'
        when CreditLimit >= 15000 then 'Mid-market'
        else 'Small'
    end as CustomerSegment,
    AccountOpenedDate,
    year(AccountOpenedDate) as AccountOpenedYear,
    IsOnCreditHold,
    EmailAddress
from {{ source('bronze', 'customers') }}
