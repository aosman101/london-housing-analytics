with base as (
    select
        cast(reference_year as int) as reference_year,
        trim(area_name) as area_name,
        trim(area_code) as area_code,
        cast(median_gross_annual_pay as numeric) as median_gross_annual_pay
    from {{ source('raw', 'ashe_earnings') }}
)
select
    reference_year,
    area_name,
    area_code,
    median_gross_annual_pay,
    round(
        100.0 * (
            median_gross_annual_pay
            / nullif(
                lag(median_gross_annual_pay) over (
                    partition by area_code
                    order by reference_year
                ),
                0
            ) - 1
        ),
        2
    ) as earnings_yoy_pct
from base
