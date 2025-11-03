
    
    

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


