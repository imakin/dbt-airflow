
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        service_zone as value_field,
        count(*) as n_records

    from "dwib"."analytics_analytics"."dim_zones"
    group by service_zone

)

select *
from all_values
where value_field not in (
    'Airports','Yellow Zone','Boro Zone','EWR','N/A'
)



  
  
      
    ) dbt_internal_test