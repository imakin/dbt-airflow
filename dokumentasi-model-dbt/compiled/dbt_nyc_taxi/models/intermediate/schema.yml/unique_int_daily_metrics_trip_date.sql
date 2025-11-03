
    
    

select
    trip_date as unique_field,
    count(*) as n_records

from "dwib"."analytics_intermediate"."int_daily_metrics"
where trip_date is not null
group by trip_date
having count(*) > 1


