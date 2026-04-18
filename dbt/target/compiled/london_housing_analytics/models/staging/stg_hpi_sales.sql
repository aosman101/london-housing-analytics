select
    cast(date_month as date) as date_month,
    trim(area_name) as area_name,
    trim(area_code) as area_code,
    cast(sales_volume as numeric) as sales_volume
from "housing_warehouse"."raw"."hpi_sales"