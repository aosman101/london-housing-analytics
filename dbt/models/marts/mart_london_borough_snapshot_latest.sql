-- Latest month per borough, plus the most recent published sales volume
-- (HPI sales lag prices by ~2 months, so the latest price month has no sales yet).
with monthly as (
    select * from {{ ref('mart_london_affordability_monthly') }}
),
latest_month as (
    select max(date_month) as latest_month
    from monthly
    where price_to_earnings_ratio is not null
),
latest as (
    select m.*
    from monthly m
    where m.date_month = (select latest_month from latest_month)
),
latest_sales as (
    select distinct on (area_code)
        area_code,
        date_month as sales_month,
        sales_volume
    from {{ ref('stg_hpi_sales') }}
    where sales_volume is not null
    order by area_code, date_month desc
)
select
    l.*,
    ls.sales_month as latest_sales_month,
    ls.sales_volume as latest_sales_volume,
    rank() over (order by l.price_to_earnings_ratio desc) as price_to_earnings_rank,
    rank() over (
        order by l.rent_growth_minus_income_growth_pct desc nulls last
    ) as rental_pressure_rank
from latest l
left join latest_sales ls
    on l.area_code = ls.area_code
