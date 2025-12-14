/*---------------------------------STEP 4: Create raw tables----------------------------*/      
create or replace table aviv.bronze.property_listings_raw
(
  listing_id string,
  property_type string,
  city string,
  region string,
  price number(38,0),
  created_at timestamp_ntz,
  updated_at timestamp_ntz,
  agent_id string,
  ingested_at timestamp_ntz default current_timestamp(),
  file_name string
);

create or replace table aviv.bronze.leads_contacts_raw
(
  contact_id string,
  listing_id string,
  contact_source string,
  contact_timestamp timestamp_ntz,
  ingested_at timestamp_ntz default current_timestamp(),
  file_name string
);

/*---------------------------------STEP 5: Load CSV----------------------------*/     
list @aviv.bronze.ingest_stg;

alter table aviv.bronze.property_listings_raw
  add column if not exists snapshot_ts timestamp_ntz;

/*---------------------------------STEP 6: Create silver table----------------------------*/    
create or replace table aviv.silver.dim_property_listing (
  listing_sk number autoincrement start 1 increment 1,
  listing_id string not null,
  property_type string,
  city string,
  region string,
  price number(38,0),
  agent_id string,
  source_created_at timestamp_ntz,
  source_updated_at timestamp_ntz,
  -- SCD2 fields
  effective_from timestamp_ntz not null,
  effective_to timestamp_ntz,
  is_current boolean not null,
  is_active boolean not null,

  -- lineage
  batch_ts timestamp_ntz not null,
  file_name string,
  ingested_at timestamp_ntz not null
);

------------------add data to RAW table
copy into aviv.bronze.property_listings_raw
(
  listing_id,
  property_type,
  city,
  region,
  price,
  created_at,
  updated_at,
  agent_id,
  ingested_at,
  file_name,
  snapshot_ts
)
from (
  select
    $1,
    $2,
    $3,
    $4,
    try_to_number($5),
    $6::timestamp_ntz,
    $7::timestamp_ntz,
    $8,
    current_timestamp(),
    metadata$filename,
    to_timestamp_ntz(
      regexp_substr(metadata$filename, '\\d{4}_\\d{2}_\\d{2}_\\d{6}'),
      'YYYY_MM_DD_HH24MISS'
    ) as snapshot_ts
  from @aviv.bronze.ingest_stg
)
pattern = '.*property_listings_.*\\.csv'
on_error = continue;




----keep the last inserted data

set snap_ts = (
  select max(snapshot_ts)
  from aviv.bronze.property_listings_raw
);

--build latest snapshot (dedup)
create or replace temporary table aviv.silver._latest_listings_batch as
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
from aviv.bronze.property_listings_raw
where snapshot_ts = $snap_ts
qualify row_number() over (
  partition by listing_id
  order by source_updated_at desc
) = 1;



--mark as not current if something got changed 
update aviv.silver.dim_property_listing d
set
  effective_to = $snap_ts,
  is_current = false
from aviv.silver._latest_listings_batch s
where d.listing_id = s.listing_id
  and d.is_current = true
  and (
       nvl(d.property_type,'') <> nvl(s.property_type,'')
    or nvl(d.city,'')         <> nvl(s.city,'')
    or nvl(d.region,'')       <> nvl(s.region,'')
    or nvl(d.price,-1)        <> nvl(s.price,-1)
    or nvl(d.agent_id,'')     <> nvl(s.agent_id,'')
  );


  --insert rows (new or changed)

  insert into aviv.silver.dim_property_listing (
  listing_id, property_type, city, region, price, agent_id,
  source_created_at, source_updated_at,
  effective_from, effective_to, is_current, is_active,
  batch_ts, file_name, ingested_at
)
select
  s.listing_id, s.property_type, s.city, s.region, s.price, s.agent_id,
  s.source_created_at, s.source_updated_at,
  s.snapshot_ts,
  null,
  true,
  true,
  s.snapshot_ts,
  s.file_name,
  s.ingested_at
from aviv.silver._latest_listings_batch s
left join aviv.silver.dim_property_listing d
  on d.listing_id = s.listing_id
 and d.is_current = true
where d.listing_id is null
   or (
       nvl(d.property_type,'') <> nvl(s.property_type,'')
    or nvl(d.city,'')         <> nvl(s.city,'')
    or nvl(d.region,'')       <> nvl(s.region,'')
    or nvl(d.price,-1)        <> nvl(s.price,-1)
    or nvl(d.agent_id,'')     <> nvl(s.agent_id,'')
   );



--Mark missing listings as inactive

update aviv.silver.dim_property_listing d
set
  is_active = false,
  effective_to = $batch_ts,
  is_current = false
where d.is_current = true
  and d.is_active = true
  and not exists (
    select 1
    from aviv.silver._latest_listings_batch s
    where s.listing_id = d.listing_id
  );


create or replace table aviv.gold.dim_property_listings as
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
  batch_ts as snapshot_ts
from aviv.silver.dim_property_listing
where is_current = true
  and is_active = true;