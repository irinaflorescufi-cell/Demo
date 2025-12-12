
{% snapshot snap_property_listings_scd2 %}
{{
  config(
    target_schema='silver',
    unique_key='listing_id',
    strategy='check',
    check_cols=['property_type','city','region','price','agent_id'],
    invalidate_hard_deletes=true
  )
}}

select
  listing_id,
  property_type,
  city,
  region,
  price,
  agent_id,
  source_created_at,
  source_updated_at,
  snapshot_ts,
  ingested_at,
  file_name
from {{ ref('stg_property_listings') }}

{% endsnapshot %}
