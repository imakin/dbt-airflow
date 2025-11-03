
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        rate_code_id as value_field,
        count(*) as n_records

    from "dwib"."analytics_staging"."stg_taxi_trips"
    group by rate_code_id

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6'
)



  
  
      
    ) dbt_internal_test