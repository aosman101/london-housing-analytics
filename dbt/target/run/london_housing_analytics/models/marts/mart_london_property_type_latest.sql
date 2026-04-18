
  
    

  create  table "housing_warehouse"."analytics"."mart_london_property_type_latest__dbt_tmp"
  
  
    as
  
  (
    with latest_month as (
    select max(date_month) as latest_month
    from "housing_warehouse"."analytics"."stg_hpi_property_type_prices"
),
prices as (
    select *
    from "housing_warehouse"."analytics"."stg_hpi_property_type_prices"
    where date_month = (select latest_month from latest_month)
),
earnings as (
    select * from "housing_warehouse"."analytics"."stg_ashe_earnings"
)
select
    p.date_month,
    p.area_code,
    p.area_name,
    p.property_type,
    p.average_price,
    e.reference_year as earnings_year,
    e.median_gross_annual_pay,
    round(p.average_price / nullif(e.median_gross_annual_pay, 0), 2) as price_to_earnings_ratio
from prices p
left join lateral (
    select reference_year, median_gross_annual_pay
    from earnings e
    where e.area_code = p.area_code
      and e.reference_year <= extract(year from p.date_month)
    order by e.reference_year desc
    limit 1
) e on true
  );
  