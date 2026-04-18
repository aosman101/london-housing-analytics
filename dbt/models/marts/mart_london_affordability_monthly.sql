with prices as (
    select * from {{ ref('stg_hpi_average_prices') }}
),
rents as (
    select * from {{ ref('stg_pipr_local_rents') }}
),
sales as (
    select * from {{ ref('stg_hpi_sales') }}
),
earnings as (
    select * from {{ ref('stg_ashe_earnings') }}
)

select
    p.date_month,
    p.area_code,
    p.area_name,
    p.average_price,
    p.hpi_index,
    p.house_price_yoy_pct,
    s.sales_volume,
    r.avg_monthly_rent,
    r.rent_yoy_pct,
    e.reference_year as earnings_year,
    e.median_gross_annual_pay,
    e.earnings_yoy_pct,
    round(p.average_price / nullif(e.median_gross_annual_pay, 0), 2) as price_to_earnings_ratio,
    round((r.avg_monthly_rent * 12) / nullif(e.median_gross_annual_pay, 0), 2) as annual_rent_to_earnings_ratio,
    round((p.average_price * 0.10) / nullif(e.median_gross_annual_pay / 12.0, 0), 1) as months_to_save_10pct_deposit,
    round(r.rent_yoy_pct - e.earnings_yoy_pct, 2) as rent_growth_minus_income_growth_pct,
    round(p.house_price_yoy_pct - e.earnings_yoy_pct, 2) as house_price_growth_minus_income_growth_pct
from prices p
left join sales s
    on p.area_code = s.area_code
   and p.date_month = s.date_month
left join rents r
    on p.area_code = r.area_code
   and p.date_month = r.date_month
join lateral (
    select
        reference_year,
        median_gross_annual_pay,
        earnings_yoy_pct
    from earnings e
    where e.area_code in (p.area_code, 'E12000007')
      and e.median_gross_annual_pay is not null
    order by
        case when e.area_code = p.area_code then 0 else 1 end,
        e.reference_year desc
    limit 1
) e on true
