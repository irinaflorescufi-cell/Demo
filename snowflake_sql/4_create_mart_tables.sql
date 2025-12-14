
  create or replace table aviv.gold.mart_daily_leads as
select
  date(f.contact_timestamp) as lead_date,
  d.region,
  d.property_type,
  f.contact_source,
  count(distinct f.contact_id) as leads_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings d
  on f.listing_id = d.listing_id
group by 1,2,3,4;


create or replace table aviv.gold.kpi_leads_per_active_listing as
select
  l.region,
  l.property_type,
  count(f.contact_id) as leads_cnt,
  count(distinct f.listing_id) as listing_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings l
  on f.listing_id = l.listing_id
group by l.region, l.property_type;



create or replace table aviv.gold.mart_daily_leads_per_listing as
select
  date(f.contact_timestamp) as lead_date,
  l.region,
  l.property_type,
  count(distinct f.contact_id) as leads_cnt,
  count(distinct l.listing_id) as active_listing_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings l
  on f.listing_id = l.listing_id
group by lead_date, l.region, l.property_type;





create or replace view aviv.reporting.leads_per_active_listing_summary as
select
  region,
  property_type,
  leads_cnt,
  listing_cnt
from aviv.gold.kpi_leads_per_active_listing
select * from aviv.gold.kpi_leads_per_active_listing

create or replace view aviv.reporting.leads_per_active_listing_detailed as
select
  lead_date,
  region,
  property_type,
  leads_cnt,
  active_listing_cnt
from aviv.gold.mart_daily_leads_per_listing


create or replace table aviv.reporting.leads_per_active_listing_snapshot as
select
  region,
  property_type,
  leads_cnt,
  listing_cnt
from aviv.gold.kpi_leads_per_active_listing