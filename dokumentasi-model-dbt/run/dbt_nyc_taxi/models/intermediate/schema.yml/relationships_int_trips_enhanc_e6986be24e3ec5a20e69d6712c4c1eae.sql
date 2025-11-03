
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select cancelled_trip_id as from_field
    from (select * from "dwib"."analytics_intermediate"."int_trips_enhanced" where cancelled_trip_id IS NOT NULL) dbt_subquery
    where cancelled_trip_id is not null
),

parent as (
    select trip_id as to_field
    from "dwib"."analytics_intermediate"."int_trips_enhanced"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test