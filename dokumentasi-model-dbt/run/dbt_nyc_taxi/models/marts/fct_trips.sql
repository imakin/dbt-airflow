
  
    
    

    create  table
      "dwib"."analytics_analytics"."fct_trips__dbt_tmp"
  
    as (
      

-- Fact table with dimensions & measures from int_trips_enhanced

with trips_enhanced as (
    select * from "dwib"."analytics_intermediate"."int_trips_enhanced"
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by trip_id 
            order by total_amount DESC
        ) as row_num
    from trips_enhanced
)

select
    trip_id,
    pickup_location_id,
    payment_type,
    pickup_datetime,
    trip_date,
    pickup_hour,
    pickup_borough,
    trip_type,
    trip_duration_minutes,
    average_speed_mph,
    total_amount
from deduplicated
where row_num = 1
    );
  
  