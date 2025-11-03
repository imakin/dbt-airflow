
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_day_of_week
from "dwib"."analytics_intermediate"."int_trips_enhanced"
where pickup_day_of_week is null



  
  
      
    ) dbt_internal_test