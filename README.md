# Snowflake + dbt


## What’s inside

### Data layers
- **Bronze (source):** raw snapshot files landed in Snowflake (`bronze.PROPERTY_LISTINGS_RAW`)
- **Staging:** cleaned + normalized + deduplicated latest snapshot (`stg_property_listings`)
- **Snapshot (SCD2):** historical tracking of listing attribute changes (`snap_property_listings_scd2`)
- **Silver:** SCD2 dimension with surrogate keys and effective dates (`dim_property_listings_scd2`)
- **Marts (Gold):** current “active” dimension for reporting (`dim_property_listings_dbt`)

### Grains
- `stg_property_listings`: 1 row per `listing_id` for the latest batch
- `snap_property_listings_scd2`: SCD2 history per `listing_id`
- `dim_property_listings_scd2`: SCD2 dimension rows (one per change period)
- `dim_property_listings`: 1 current row per `listing_id`

## How to run

### 1) Install deps
dbt run
dbt snapshot
dbt test