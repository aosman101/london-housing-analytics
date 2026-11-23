# Connecting Tableau

The dbt marts are built for BI tools. Connect Tableau (or Power BI) straight to PostgreSQL, or use
the CSV exports if you are not running the database.

## Option 1: live PostgreSQL connection

1. Run the pipeline so the marts exist: `make up && make all`.
2. In Tableau, choose **Connect → To a Server → PostgreSQL** and enter the values from your `.env`:

   | Field | Value (defaults from `.env.example`) |
   | --- | --- |
   | Server | `localhost` |
   | Port | `5432` |
   | Database | `housing_warehouse` |
   | Username / Password | `analytics` / `analytics` |

3. Pick the **`analytics`** schema and drag in one of the marts:

   | Mart | Grain | Good for |
   | --- | --- | --- |
   | `mart_london_borough_snapshot_latest` | borough | Maps, rankings, KPI tiles |
   | `mart_london_affordability_monthly` | borough × month | Price, rent and sales trends |
   | `mart_london_property_type_latest` | borough × property type | Affordability gap by property type |

**macOS driver.** Tableau needs the PostgreSQL JDBC driver. If the connection fails with a driver
error, download the `.jar` from <https://jdbc.postgresql.org/download/> and copy it to
`~/Library/Tableau/Drivers`, then restart Tableau.

## Option 2: CSV exports

`make export` writes each mart to `data/exports/*.csv`. Use **Connect → To a File → Text file**.
Re-run `make export` after every pipeline refresh, because the CSVs are not tracked in git.

## Borough maps

The repository ships borough geography in [`data/spatial`](../data/spatial):

- `london_borough_boundaries_only.geojson`: add it as a **Spatial file** and relate `LAD24CD` to
  the mart's `area_code` for filled maps.
- `london_borough_centroids.csv`: `area_code`, `longitude`, `latitude`, for point maps or labels.

## Things to know before you build

- Earnings-based ratios are only populated from 2025 onwards (one ASHE release is loaded), so
  filter trend views of ratios to 2025+.
- Use `earnings_quality` to flag less precise borough pay estimates, and `earnings_fallback_used`
  to flag the City of London, which uses London-wide pay.
- The City of London has no rent series and only a flat price by property type.

See the [project README](../README.md#kpi-definitions) for every KPI definition.
