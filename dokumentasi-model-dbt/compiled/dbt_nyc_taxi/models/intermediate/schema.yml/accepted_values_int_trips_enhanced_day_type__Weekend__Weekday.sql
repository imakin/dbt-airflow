
    
    

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


