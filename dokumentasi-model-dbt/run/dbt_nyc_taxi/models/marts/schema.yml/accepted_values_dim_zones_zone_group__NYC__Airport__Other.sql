
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        zone_group as value_field,
        count(*) as n_records

    from "dwib"."analytics_analytics"."dim_zones"
    group by zone_group

)

select *
from all_values
where value_field not in (
    'NYC','Airport','Other'
)



  
  
      
    ) dbt_internal_test