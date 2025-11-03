
  
    
    

    create  table
      "dwib"."analytics_analytics"."dim_zones__dbt_tmp"
  
    as (
      

-- Dimension table for NYC Taxi Zones with hierarchy

with zones_staging as (
    select * from "dwib"."analytics_staging"."stg_taxi_zones"
)

select
    location_id,
    zone as zone_name,
    borough,
    -- Hierarchy (soal: "dengan hierarchies")
    case 
        when borough = 'Manhattan' then 1
        when borough = 'Brooklyn' then 2
        when borough = 'Queens' then 3
        when borough = 'Bronx' then 4
        when borough = 'Staten Island' then 5
        when borough = 'EWR' then 6
        else 99
    end as borough_sort_order
from zones_staging
order by borough_sort_order, zone_name
    );
  
  