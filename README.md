# Snowflake + dbt

## Context & approach

The goal of this case study was to design a simple but realistic analytics data platform, focusing on data modeling, data quality, and business-oriented analytics.

### Approach

The core transformations and business logic were **first implemented and validated directly in Snowflake**.
This allowed fast iteration, easy debugging, and full control over complex logic such as SCD Type 2 handling, deduplication, and business KPIs.

Once the logic was stable and validated, **part of the solution was replicated in dbt** to demonstrate how the same pipeline would be structured in a production-grade analytics project: staging models, SCD2 snapshots, curated dimensions, marts, and data quality tests.

## Repository structure

```
├── snowflake_sql/        # Snowflake-first implementation and validation
│   ├── 01_Initialization.sql            # Snowflake objects, stages, file formats
│   ├── 02_property_listings_flow.sql    # Listings pipeline (SCD2 dimension)
│   ├── 03_leads_contacts.sql            # Leads / contacts fact pipeline
│   ├── 04_create_mart_tables.sql        # Aggregated marts and KPIs
│   └── 05_Queries_for_analysis.sql      # Business-driven analytical queries
│
├── models/               # dbt models (staging, silver, marts)
├── snapshots/            # dbt SCD2 snapshots
├── tests/                # dbt data quality & logic tests
├── seeds/
└── README.md
```

## Snowflake-first implementation

The `snowflake_sql/` folder contains the SQL scripts used to design, test, and validate the full data pipeline directly in Snowflake, by loading the CSVs from the same folder.
These scripts represent the **business logic** for the case study.

### 01_Initialization.sql
- Creates file formats and internal stages
- Defines raw Bronze tables for property listings and leads/contacts
- Handles CSV ingestion and snapshot timestamp extraction from file names

### 02_property_listings_flow.sql
- Builds the full listings pipeline:
  - Latest snapshot extraction and deduplication
  - SCD Type 2 logic (effective_from / effective_to)
  - Detection of attribute changes
  - Handling of inactive (disappeared) listings
- Produces:
  - `silver.dim_property_listing` (SCD2 dimension)
  - `gold.dim_property_listings` (current active listings)

### 03_leads_contacts.sql
- Loads raw leads/contacts data
- Validates referential integrity against active listings
- Separates valid data from quarantined records
- Builds:
  - `silver.fact_leads_contacts`
  - `gold.fact_leads_contacts_active`

### 04_create_mart_tables.sql
- Builds analytics-ready marts and KPIs:
  - Daily leads
  - Leads per active listing
  - Leads by region and property type
- Creates reporting views for consumption

### 05_Queries_for_analysis.sql
This file contains exploratory and business-driven queries used to answer questions such as:
- Which listings underperform (few or no leads)?
- Time to first lead after listing publication
- Impact of price changes on lead volume
- High-conversion regions and property types
- Demand vs supply indicators

## dbt implementation

The dbt project mirrors part of the Snowflake logic and shows how this pipeline would be structured in a production analytics environment:

- Sources and staging models
- SCD Type 2 snapshots for listings
- Curated silver dimensions
- Gold marts for reporting
- Data quality and SCD2 integrity tests


## Next steps (given more time)

Given more time, the solution could be further strengthened by:

- **Extending dbt coverage**  
  Gradually moving more Snowflake logic into dbt models once the transformations are fully stabilized.

- **Automation & orchestration**  
  Scheduling regular runs (e.g. via tasks / orchestration tools) to automate ingestion, transformations, and snapshots.

- **Analytics consumption layer**  
  Defining dbt exposures for BI dashboards and downstream consumers.

- **Scalability improvements**  
  Introducing incremental models and partitioning strategies for larger datasets.

- **Data quality & governance**  
  Expanding data quality checks, SCD2 integrity tests, and freshness monitoring.

- **Documentation & evolution**  
  Enhancing documentation and adapting the models as new business requirements and attributes are introduced.
