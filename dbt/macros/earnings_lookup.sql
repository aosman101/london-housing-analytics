{#
  Lateral lookup for the most relevant ASHE earnings row for a borough-month:
  prefer the borough's own estimate, fall back to the London region, and only
  accept reference years in [year(date_month) - earnings_max_lag_years, year(date_month)].
  Usage: left join lateral ({{ earnings_lookup('p') }}) e on true
#}
{% macro earnings_lookup(alias) %}
    select
        area_code,
        reference_year,
        median_gross_annual_pay,
        earnings_yoy_pct,
        earnings_quality
    from {{ ref('stg_ashe_earnings') }} e
    where e.area_code in ({{ alias }}.area_code, '{{ var("london_region_code") }}')
      and e.reference_year <= extract(year from {{ alias }}.date_month)
      and e.reference_year >= extract(year from {{ alias }}.date_month) - {{ var("earnings_max_lag_years") }}
      and e.median_gross_annual_pay is not null
    order by
        case when e.area_code = {{ alias }}.area_code then 0 else 1 end,
        e.reference_year desc
    limit 1
{% endmacro %}
