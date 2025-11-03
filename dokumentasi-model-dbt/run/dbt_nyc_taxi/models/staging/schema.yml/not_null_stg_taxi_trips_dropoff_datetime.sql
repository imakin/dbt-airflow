
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dropoff_datetime
from "dwib"."analytics_staging"."stg_taxi_trips"
where dropoff_datetime is null



  
  
      
    ) dbt_internal_test