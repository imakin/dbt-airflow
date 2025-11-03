
    
    

select
    trip_id as unique_field,
    count(*) as n_records

from "dwib"."analytics_intermediate"."int_trips_enhanced"
where trip_id is not null
group by trip_id
having count(*) > 1


