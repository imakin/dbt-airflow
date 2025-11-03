
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_trips
from "dwib"."analytics_analytics"."agg_daily_stats"
where total_trips is null



  
  
      
    ) dbt_internal_test