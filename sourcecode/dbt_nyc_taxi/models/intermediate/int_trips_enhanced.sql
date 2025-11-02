{{ config(
    materialized = "view",
    schema = "intermediate"
)}}

-- Enhanced trips with zones, calculations, and time dimensions

with
trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

enhanced as (
    select
        trips.trip_id,
        trips.pickup_location_id,
        trips.payment_type,
        trips.total_amount,
        trips.pickup_datetime,
        
        -- Join with zones (requirement: "Join with zones")
        zone_pickup.borough as pickup_borough,
        
        -- Calculate duration (requirement: "calculate duration")
        datediff('minute', trips.pickup_datetime, trips.dropoff_datetime) as trip_duration_minutes,
        
        -- Calculate speed (requirement: "calculate speed")
        case 
            when datediff('minute', trips.pickup_datetime, trips.dropoff_datetime) > 0 
            then round(trips.trip_distance / (datediff('minute', trips.pickup_datetime, trips.dropoff_datetime) / 60.0), 2)
            else null
        end as average_speed_mph,
        
        -- Categorize trips (requirement: "categorize trips")
        case
            when zone_pickup.borough = zone_dropoff.borough then 'Intra-Borough'
            when zone_pickup.borough = 'EWR' or zone_dropoff.borough = 'EWR' then 'Airport'
            else 'Inter-Borough'
        end as trip_type,
        
        -- Time dimensions (requirement: "add time dimensions")
        extract(hour from trips.pickup_datetime) as pickup_hour,
        date_trunc('day', trips.pickup_datetime) as trip_date
        
    from trips
    left join zones as zone_pickup 
        on trips.pickup_location_id = zone_pickup.location_id
    left join zones as zone_dropoff 
        on trips.dropoff_location_id = zone_dropoff.location_id
    where trips.total_amount > 0
)

select * from enhanced