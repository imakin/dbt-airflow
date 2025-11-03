
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fare_amount
from "dwib"."analytics_staging"."stg_taxi_trips"
where fare_amount is null



  
  
      
    ) dbt_internal_test