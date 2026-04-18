select
    cast(date_month as date) as date_month,
    trim(area_name) as area_name,
    trim(area_code) as area_code,
    cast(avg_monthly_rent as numeric) as avg_monthly_rent,
    cast(rent_yoy_pct as numeric) as rent_yoy_pct
from "housing_warehouse"."raw"."pipr_local_rents"