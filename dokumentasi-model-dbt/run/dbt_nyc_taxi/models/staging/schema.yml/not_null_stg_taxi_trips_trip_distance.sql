
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_distance
from "dwib"."analytics_staging"."stg_taxi_trips"
where trip_distance is null



  
  
      
    ) dbt_internal_test