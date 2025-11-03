
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_revenue
from "dwib"."analytics_intermediate"."int_daily_metrics"
where total_revenue is null



  
  
      
    ) dbt_internal_test