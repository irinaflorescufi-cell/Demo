-- Fail if any listing_id has more than one current row
select
  listing_id
from {{ ref('dim_property_listings_scd2') }}
where effective_to is null
group by listing_id
having count(1) > 1
