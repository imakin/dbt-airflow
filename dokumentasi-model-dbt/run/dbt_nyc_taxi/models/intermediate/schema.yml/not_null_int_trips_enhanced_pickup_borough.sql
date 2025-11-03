
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_borough
from "dwib"."analytics_intermediate"."int_trips_enhanced"
where pickup_borough is null



  
  
      
    ) dbt_internal_test