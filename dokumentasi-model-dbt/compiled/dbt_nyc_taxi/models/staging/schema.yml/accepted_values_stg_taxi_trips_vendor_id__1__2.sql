
    
    

with all_values as (

    select
        vendor_id as value_field,
        count(*) as n_records

    from "dwib"."analytics_staging"."stg_taxi_trips"
    group by vendor_id

)

select *
from all_values
where value_field not in (
    '1','2'
)


