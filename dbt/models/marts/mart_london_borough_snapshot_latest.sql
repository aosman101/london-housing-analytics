with latest_month as (
    select max(date_month) as latest_month
    from {{ ref('mart_london_affordability_monthly') }}
)
select *
from {{ ref('mart_london_affordability_monthly') }}
where date_month = (select latest_month from latest_month)
