
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    trip_date as unique_field,
    count(*) as n_records

from "dwib"."analytics_intermediate"."int_daily_metrics"
where trip_date is not null
group by trip_date
having count(*) > 1



  
  
      
    ) dbt_internal_test