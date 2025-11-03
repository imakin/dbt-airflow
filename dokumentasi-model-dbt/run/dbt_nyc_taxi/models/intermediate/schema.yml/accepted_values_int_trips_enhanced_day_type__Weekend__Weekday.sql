
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        day_type as value_field,
        count(*) as n_records

    from "dwib"."analytics_intermediate"."int_trips_enhanced"
    group by day_type

)

select *
from all_values
where value_field not in (
    'Weekend','Weekday'
)



  
  
      
    ) dbt_internal_test