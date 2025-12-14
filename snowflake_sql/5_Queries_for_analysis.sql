

/*---------------------------------------------------------------Business case -------------------------------------------------------------
Detect under-performing listings - Which listings receive fewer leads than expected (show top-to-bottom, bottom-to-top)
It can help the company to decide where to improve listing quality, alert agents, etc
Done with left join because there can be listings without any leads -> CRITICAL for decision-making
*/
select d.listing_id, count(f.contact_id)
from aviv.gold.dim_property_listings d  
left join aviv.gold.fact_leads_contacts_active f
on f.listing_id = d.listing_id
group by d.listing_id
order by count(d.listing_id); --bottom-to-top

select d.listing_id, count(f.contact_id)
from aviv.gold.dim_property_listings d  
left join aviv.gold.fact_leads_contacts_active f
on f.listing_id = d.listing_id
group by d.listing_id
order by count(d.listing_id) desc; --bottom-to-top



/*---------------------------------------------------------------Business case -------------------------------------------------------------
Identify high-conversion regions to answer to question "Where do listings attract more leads per region"
It can help the company to decide where to invest marketing, prioritize agent onboarding (more leads), strong vs weak market
*/
select d.region, count(f.contact_id) as leads_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings d 
on f.listing_id = d.listing_id
group by d.region


/*---------------------------------------------------------------Business case-------------------------------------------------------------
Optimize marketing spend to answer to question "Which channels bring the most leads"
It can help the company to decide where to allocate marketing budget, reduce spend on low-performing channels
*/
select f.contact_source, count(f.contact_id) as leads_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings d 
on f.listing_id = d.listing_id
group by f.contact_source
order by  count(f.contact_id) desc 

/*---------------------------------------------------------------Business case-------------------------------------------------------------
Property type - what are the property types that receive more leads
*/
select d.property_type, count(f.contact_id) as leads_cnt
from aviv.gold.fact_leads_contacts_active f
join aviv.gold.dim_property_listings d 
on f.listing_id = d.listing_id
group by d.property_type
order by  count(d.property_type) desc 

/*---------------------------------------------------------------Business case-------------------------------------------------------------
Market reaction - time to first lead. Measures how quickly a new listing attracts its first lead after being published
Short time  = strong demand and good listing quality (price, photos, description, location, etc)
Long time = listing that needs action
This can then be used as aggregated by region/type or 
*/
with first_lead as (
  select
    listing_id,
    min(contact_timestamp) as first_lead_ts
  from aviv.gold.fact_leads_contacts_active
  group by listing_id
)
select t.listing_id
        ,t.property_type
        ,t.region
        ,datediff(day, t.source_created_at, current_timestamp()) as days_live
        ,datediff(hour, t.source_created_at, f.first_lead_ts) as diff_hours_listed_to_lead
from aviv.gold.dim_property_listings t
left join first_lead f
on t.listing_id = f.listing_id

/*---------------------------------------------------------------Business case-------------------------------------------------------------
Trend - the results can be used in a chart to show the leads per day for each active listing. 
*/
  select
    date(contact_timestamp) as lead_date,
    listing_id,
    count(1) as leads
  from aviv.gold.fact_leads_contacts_active
  group by 1,2
  order by lead_date desc 


/*---------------------------------------------------------------Business case-------------------------------------------------------------
Price-change impact on lead
*/
with price_changes as (
--calculate previous price, if exists
  select
    listing_id,
    effective_from as change_ts,
    price,
    lag(price) over (partition by listing_id order by effective_from) as prev_price
  from aviv.silver.dim_property_listing
),
drops as (
--keep only the listings for which the price has changed
  select p.*, price - prev_price diff_price, case when price - prev_price < 0 then 'DECREASED' else 'INCREASED' end alert_price , 
  from price_changes p
  where prev_price is not null and price < prev_price
),
leads_around as (
--identify the number of leads before 3 days of price changed, after 3 days of price changed
  select
    d.listing_id,
    d.change_ts,
    count_if(f.contact_timestamp >= d.change_ts - interval '3 day'
             and f.contact_timestamp <  d.change_ts) as leads_before_3d,
    count_if(f.contact_timestamp >= d.change_ts
             and f.contact_timestamp <  d.change_ts + interval '3 day') as leads_after_3d
  from drops d
  left join aviv.silver.fact_leads_contacts f
    on f.listing_id = d.listing_id
  group by 1,2
)
select * from leads_around;


/*---------------------------------------------------------------Business case-------------------------------------------------------------
Price monitoring - it can be used for alerts
*/
with price_changes as (
--calculate previous price, if exists
  select
    listing_id,
    effective_from as change_ts,
    price,
    lag(price) over (partition by listing_id order by effective_from) as prev_price
  from aviv.silver.dim_property_listing
),
changes as (
  select
    listing_id,
    change_ts,
    price,
    prev_price,
    price - prev_price as diff_price,
    case
      when price < prev_price then 'DECREASED'
      when price > prev_price then 'INCREASED'
      else 'UNCHANGED'
    end as price_alert
  from price_changes
  
)
select
  listing_id,
  change_ts,
  price as actual_price,
  prev_price as previous_price,
  diff_price,
  price_alert
from changes
order by change_ts desc, listing_id;

/*---------------------------------------------------------------Business case-------------------------------------------------------------
High conversion regions
*/
with supply as (
--active supply
  select region, property_type, count(*) as active_listings
  from aviv.gold.dim_property_listings
  group by 1,2
),
demand as (
--actual demand
  select d.region, d.property_type, count(*) as leads
  from aviv.gold.fact_leads_contacts_active f
  join aviv.gold.dim_property_listings d on f.listing_id = d.listing_id
  group by 1,2
),
total_counts as (
  select
    (select sum(active_listings) from supply) as total_supply,
    (select sum(leads) from demand) as total_demand
)
--analysis, shares
select
  s.region, s.property_type,
  s.active_listings,
  coalesce(dm.leads,0) as leads,
  round(coalesce(dm.leads,0) / nullif(t.total_demand,0), 4) as demand_share,
  round(s.active_listings / nullif(t.total_supply,0), 4) as supply_share,
  round((coalesce(dm.leads,0) / nullif(t.total_demand,0)) /
        nullif((s.active_listings / nullif(t.total_supply,0)),0), 4) as demand_supply_index
from supply s
left join demand dm using (region, property_type)
cross join total_counts t
order by demand_supply_index desc;


/*---------------------------------------------------------------Business case-------------------------------------------------------------
Detect listings with low performance (alerts/monitoring)
Active listings but no leads for 7 days
*/
with last_lead as (
  select listing_id, max(contact_timestamp) as last_lead_ts
  from aviv.gold.fact_leads_contacts_active
  group by listing_id
)
select
  d.listing_id, d.region, d.property_type, d.price,
  l.last_lead_ts,
  datediff('day', l.last_lead_ts, current_timestamp()) as days_since_last_lead
from aviv.gold.dim_property_listings d
left join last_lead l on d.listing_id = l.listing_id
where l.last_lead_ts is null
   or datediff('day', l.last_lead_ts, current_timestamp()) >= 7
order by days_since_last_lead desc nulls last;


