
    
    

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
    'Intra-Borough','Inter-Borough','Airport','Unknown'
)


