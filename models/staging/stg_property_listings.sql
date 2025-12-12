with src as (
  select *
  from {{ source('bronze', 'PROPERTY_LISTINGS_RAW') }}
),

latest as (
  select max(snapshot_ts) as snap_ts
  from src
),

clean as (
  select
    trim(listing_id) as listing_id,
    lower(trim(property_type)) as property_type,
    trim(city) as city,
    upper(trim(region)) as region,
    try_to_number(price) as price,
    trim(agent_id) as agent_id,
    created_at::timestamp_ntz as source_created_at,
    updated_at::timestamp_ntz as source_updated_at,
    snapshot_ts,
    ingested_at,
    file_name
  from src
  where snapshot_ts = (select snap_ts from latest)
),

dedup as (
  select *
  from clean
  qualify row_number() over (
    partition by listing_id
    order by source_updated_at desc
  ) = 1
)

select * from dedup

