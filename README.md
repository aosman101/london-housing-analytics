# london-housing-analytics

London Housing Affordability & Rental Pressure Analytics Warehouse


# Project Architecture

HM Land Registry HPI CSVs
ONS PIPR XLSX
ONS ASHE Table 8 ZIP/CSV
ONS LAD 2024 boundary GeoJSON
        |
        v
Python extract + normalise
        |
        v
PostgreSQL raw schema
        |
        v
dbt staging models
        |
        v
London marts
  - mart_london_affordability_monthly
  - mart_london_property_type_latest
  - mart_london_borough_snapshot_latest
        |
        v
Tableau Desktop / Tableau Public
