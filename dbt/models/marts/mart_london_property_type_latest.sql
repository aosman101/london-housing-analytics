with latest_month as (
    select max(date_month) as latest_month
    from {{ ref('stg_hpi_property_type_prices') }}
),
prices as (
    select *
    from {{ ref('stg_hpi_property_type_prices') }}
    where date_month = (select latest_month from latest_month)
      and average_price is not null
)
select
    p.date_month,
    p.area_code,
    p.area_name,
    p.property_type,
    p.average_price,
    e.reference_year as earnings_year,
    e.median_gross_annual_pay,
    e.earnings_quality,
    (e.area_code <> p.area_code) as earnings_fallback_used,
    round(p.average_price / nullif(e.median_gross_annual_pay, 0), 2) as price_to_earnings_ratio
from prices p
join lateral ({{ earnings_lookup('p') }}) e on true
