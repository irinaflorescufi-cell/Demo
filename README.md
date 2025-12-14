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
│   ├── 01_Initialization.sql           #  creation of the objects
│   ├── 02_property_listings_flow.sql    #flow for property listings (DIM)
│   ├── 03_leads_contacts.sql            # flow for leads contacts (FACT)
│   ├── 04_create_mart_tables.sql        # Creation of some summarized tables that can be used for reporting
│   └── 05_Queries_for_analysis.sql      # Business cases that I have thought of and some explanations
│
├── models/               # dbt models (staging, silver, marts)
├── snapshots/            # dbt SCD2 snapshots
├── tests/                # dbt data quality & logic tests
├── seeds/
└── README.md
```
