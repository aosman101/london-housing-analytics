with latest_month as (
    select max(date_month) as latest_month
    from "housing_warehouse"."analytics"."mart_london_affordability_monthly"
)
select *
from "housing_warehouse"."analytics"."mart_london_affordability_monthly"
where date_month = (select latest_month from latest_month)