
    
    

select
    location_id as unique_field,
    count(*) as n_records

from "dwib"."analytics_staging"."stg_taxi_zones"
where location_id is not null
group by location_id
having count(*) > 1


