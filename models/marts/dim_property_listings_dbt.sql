select
  listing_id,
  property_type,
  city,
  region,
  price,
  agent_id,
  source_created_at,
  source_updated_at,
  effective_from as active_from,
  batch_ts
from {{ ref('dim_property_listings_scd2') }}
where effective_to is null
