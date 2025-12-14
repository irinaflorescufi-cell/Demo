
  create or replace table aviv.bronze.leads_contacts_raw
(
  contact_id string,
  listing_id string,
  contact_source string,
  contact_timestamp timestamp_ntz,
  ingested_at timestamp_ntz default current_timestamp(),
  file_name string,
  snapshot_ts timestamp_ntz
);


create or replace table aviv.silver.fact_leads_contacts (
  contact_id string not null,
  listing_id string not null,
  contact_source string not null,
  contact_timestamp timestamp_ntz not null,

  snapshot_ts timestamp_ntz,
  file_name string,
  ingested_at timestamp_ntz
);

   

copy into aviv.bronze.leads_contacts_raw
(
  contact_id,
  listing_id,
  contact_source,
  contact_timestamp,
  ingested_at,
  file_name,
  snapshot_ts
)
from (
  select
    $1,                          -- contact_id
    $2,                          -- listing_id
    $3,                          -- contact_source
    $4::timestamp_ntz,           -- contact_timestamp
    current_timestamp(),         -- ingested_at
    metadata$filename,           -- file_name
    to_timestamp_ntz(
      regexp_substr(metadata$filename, '\\d{4}_\\d{2}_\\d{2}_\\d{6}'),
      'YYYY_MM_DD_HH24MISS'
    ) as snapshot_ts
  from @aviv.bronze.ingest_stg
)
pattern = '.*leads_contacts_.*\\.csv'
on_error = continue;

create or replace table aviv.silver.fact_leads_contacts_quarantine (
  contact_id string,
  listing_id string,
  contact_source string,
  contact_timestamp timestamp_ntz,
  snapshot_ts timestamp_ntz,
  file_name string,
  ingested_at timestamp_ntz,
  quarantine_reason string
);



merge into aviv.silver.fact_leads_contacts t
using (
  select
    trim(c.contact_id) as contact_id,
    trim(c.listing_id) as listing_id,
    lower(trim(c.contact_source)) as contact_source,
    c.contact_timestamp::timestamp_ntz as contact_timestamp,
    c.snapshot_ts::timestamp_ntz as snapshot_ts,
    c.file_name as file_name,
    c.ingested_at::timestamp_ntz as ingested_at
  from aviv.bronze.leads_contacts_raw c
  join aviv.gold.dim_property_listings l
    on trim(c.listing_id) = l.listing_id
  where c.contact_id is not null
) s
on t.contact_id = s.contact_id
when matched then update set
  t.listing_id = s.listing_id,
  t.contact_source = s.contact_source,
  t.contact_timestamp = s.contact_timestamp,
  t.snapshot_ts = s.snapshot_ts,
  t.file_name = s.file_name,
  t.ingested_at = s.ingested_at
when not matched then insert (
  contact_id, listing_id, contact_source, contact_timestamp,
  snapshot_ts, file_name, ingested_at
) values (
  s.contact_id, s.listing_id, s.contact_source, s.contact_timestamp,
  s.snapshot_ts, s.file_name, s.ingested_at
);



insert into aviv.silver.fact_leads_contacts_quarantine (
  contact_id, listing_id, contact_source, contact_timestamp,
  snapshot_ts, file_name, ingested_at, quarantine_reason
)
select
  trim(c.contact_id),
  trim(c.listing_id),
  lower(trim(c.contact_source)),
  c.contact_timestamp::timestamp_ntz,
  c.snapshot_ts::timestamp_ntz,
  c.file_name,
  c.ingested_at::timestamp_ntz,
  'LISTING_NOT_FOUND'
from aviv.bronze.leads_contacts_raw c
left join aviv.gold.dim_property_listings l
  on trim(c.listing_id) = l.listing_id
where l.listing_id is null;



create or replace view aviv.gold.fact_leads_contacts_active as
select
  f.contact_id,
  f.listing_id,
  f.contact_source,
  f.contact_timestamp,
  f.snapshot_ts,
  f.file_name,
  f.ingested_at
from aviv.silver.fact_leads_contacts f
join aviv.gold.dim_property_listings d
  on f.listing_id = d.listing_id;
