select
    cast(date_month as date) as date_month,
    trim(area_name) as area_name,
    trim(area_code) as area_code,
    trim(property_type) as property_type,
    cast(average_price as numeric) as average_price,
    cast(hpi_index as numeric) as hpi_index,
    cast(pct_change_1m as numeric) as pct_change_1m,
    cast(pct_change_12m as numeric) as house_price_yoy_pct
from "housing_warehouse"."raw"."hpi_property_type_prices"