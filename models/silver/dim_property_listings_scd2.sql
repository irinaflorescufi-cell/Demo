select
  {{ dbt_utils.generate_surrogate_key(['listing_id','dbt_valid_from']) }} as listing_sk,
  listing_id,
  property_type,
  city,
  region,
  price,
  agent_id,
  source_created_at,
  source_updated_at,

  dbt_valid_from::timestamp_ntz as effective_from,
  dbt_valid_to::timestamp_ntz   as effective_to,
  (dbt_valid_to is null)        as is_current,

  -- is_active = current in latest staged snapshot
  (dbt_valid_to is null) as is_active,

  snapshot_ts as batch_ts,
  file_name,
  ingested_at

from {{ ref('snap_property_listings_scd2') }}
