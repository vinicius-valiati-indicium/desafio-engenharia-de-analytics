with sales_2011 as(
    select sum(price) as total from {{ ref('fct_sales') }}
    where order_date >= '2011-01-01' and order_date < '2012-01-01'
)

select total
from sales_2011
where total not between 12646112 and 12646113