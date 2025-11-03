
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        trip_type as value_field,
        count(*) as n_records

    from "dwib"."analytics_analytics"."fct_trips"
    group by trip_type

)

select *
from all_values
where value_field not in (
    'Intra-Borough','Inter-Borough','Airport'
)



  
  
      
    ) dbt_internal_test