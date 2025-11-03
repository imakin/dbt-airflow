
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_id
from "dwib"."analytics_analytics"."dim_zones"
where location_id is null



  
  
      
    ) dbt_internal_test