
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select dropoff_borough
from "dwib"."analytics_intermediate"."int_trips_enhanced"
where dropoff_borough is null



  
  
      
    ) dbt_internal_test