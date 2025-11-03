
    
    

with all_values as (

    select
        store_and_fwd_flag as value_field,
        count(*) as n_records

    from "dwib"."analytics_staging"."stg_taxi_trips"
    group by store_and_fwd_flag

)

select *
from all_values
where value_field not in (
    'Y','N'
)


