with source as (
    select * from {{ source('raw', 'hpi_property_type_prices') }}
),
typed as (
    select
        cast(date_month as date) as date_month,
        trim(area_name) as area_name,
        trim(area_code) as area_code,
        trim(property_type) as property_type_raw,
        cast(average_price as numeric) as average_price,
        cast(hpi_index as numeric) as hpi_index,
        cast(pct_change_1m as numeric) as pct_change_1m,
        cast(pct_change_12m as numeric) as house_price_yoy_pct
    from source
)
select
    date_month,
    area_name,
    area_code,
    case
        when property_type_raw in ('Detached', 'D') then 'Detached'
        when property_type_raw in ('Semi-detached', 'S') then 'Semi-detached'
        when property_type_raw in ('Terraced', 'T') then 'Terraced'
        when property_type_raw in ('Flat/Maisonette', 'Flat or maisonette', 'F') then 'Flat/Maisonette'
        else property_type_raw
    end as property_type,
    average_price,
    hpi_index,
    pct_change_1m,
    house_price_yoy_pct
from typed
