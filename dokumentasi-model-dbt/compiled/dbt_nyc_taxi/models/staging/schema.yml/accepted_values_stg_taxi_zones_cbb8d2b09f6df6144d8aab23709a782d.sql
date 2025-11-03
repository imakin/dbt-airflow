
    
    

with all_values as (

    select
        borough as value_field,
        count(*) as n_records

    from "dwib"."analytics_staging"."stg_taxi_zones"
    group by borough

)

select *
from all_values
where value_field not in (
    'Manhattan','Brooklyn','Queens','Bronx','Staten Island','EWR','Unknown'
)


