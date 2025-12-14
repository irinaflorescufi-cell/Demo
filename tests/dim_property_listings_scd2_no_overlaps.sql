-- Fail if SCD2 ranges overlap for the same listing_id
with s as (
  select
    listing_id,
    effective_from,
    coalesce(effective_to, '9999-12-31'::timestamp_ntz) as effective_to
  from {{ ref('dim_property_listings_scd2') }}
),
pairs as (
  select
    a.listing_id,
    a.effective_from as a_from,
    a.effective_to   as a_to,
    b.effective_from as b_from,
    b.effective_to   as b_to
  from s a
  join s b
    on a.listing_id = b.listing_id
   and a.effective_from < b.effective_from
)
select listing_id
from pairs
where a_to > b_from
