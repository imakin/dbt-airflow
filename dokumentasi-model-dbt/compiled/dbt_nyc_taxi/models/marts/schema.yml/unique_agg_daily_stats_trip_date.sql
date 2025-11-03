
    
    

select
    trip_date as unique_field,
    count(*) as n_records

from "dwib"."analytics_analytics"."agg_daily_stats"
where trip_date is not null
group by trip_date
having count(*) > 1


