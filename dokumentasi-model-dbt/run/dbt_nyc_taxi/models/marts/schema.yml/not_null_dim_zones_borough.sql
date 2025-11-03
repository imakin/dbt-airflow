
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select borough
from "dwib"."analytics_analytics"."dim_zones"
where borough is null



  
  
      
    ) dbt_internal_test