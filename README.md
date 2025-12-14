# Snowflake + dbt


## What’s inside

## Context & approach

The goal of this case study was to design a simple but realistic analytics data platform for a real-estate marketplace (SeLoger-like use case), focusing on data modeling, data quality, and business-oriented analytics.

### Approach

The core transformations and business logic were **first implemented and validated directly in Snowflake**.
This allowed fast iteration, easy debugging, and full control over complex logic such as SCD Type 2 handling, deduplication, and business KPIs.

Once the logic was stable and validated, **part of the solution was replicated in dbt** to demonstrate how the same pipeline would be structured in a production-grade analytics project: staging models, SCD2 snapshots, curated dimensions, marts, and data quality tests.

