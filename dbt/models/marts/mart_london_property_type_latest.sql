with latest_month as (
    select max(date_month) as latest_month
    from {{ ref('stg_hpi_property_type_prices') }}
),
prices as (
    select *
    from {{ ref('stg_hpi_property_type_prices') }}
    where date_month = (select latest_month from latest_month)
),
earnings as (
    select * from {{ ref('stg_ashe_earnings') }}
)
select
    p.date_month,
    p.area_code,
    p.area_name,
    p.property_type,
    p.average_price,
    e.reference_year as earnings_year,
    e.median_gross_annual_pay,
    (e.area_code <> p.area_code) as earnings_fallback_used,
    round(p.average_price / nullif(e.median_gross_annual_pay, 0), 2) as price_to_earnings_ratio
from prices p
join lateral (
    select
        area_code,
        reference_year,
        median_gross_annual_pay
    from earnings e
    where e.area_code in (p.area_code, 'E12000007')
      and e.reference_year <= extract(year from p.date_month)
      and e.median_gross_annual_pay is not null
    order by
        case when e.area_code = p.area_code then 0 else 1 end,
        e.reference_year desc
    limit 1
) e on true
