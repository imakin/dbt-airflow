
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_date
from "dwib"."analytics_intermediate"."int_daily_metrics"
where trip_date is null



  
  
      
    ) dbt_internal_test