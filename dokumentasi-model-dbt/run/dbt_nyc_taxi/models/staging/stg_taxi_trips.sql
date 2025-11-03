
  
  create view "dwib"."analytics_staging"."stg_taxi_trips__dbt_tmp" as (
    

with

source_data as (
    select *
    from
        "dwib"."raw"."yellow_tripdata"
),

-- vendorid tpep_pickup_datetime tpep_dropoff_datetime
-- passenger_count  trip_distance
-- ratecodeid store_and_fwd_flag  pulocationid
-- dolocationid  payment_type  fare_amount  extra
-- mta_tax  tip_amount  tolls_amount
-- improvement_surcharge  total_amount
-- congestion_surcharge  airport_fee

-- clean data
filtered as (
    select *
    from source_data
    WHERE
        tpep_pickup_datetime IS NOT NULL
        AND tpep_dropoff_datetime IS NOT NULL
        AND passenger_count > 0
        AND trip_distance > 0
        AND fare_amount >= 0
        AND payment_type IS NOT NULL
),

-- rename tpep_ dsb
renamed as (
    select
        vendorid as vendor_id,
        tpep_pickup_datetime   as pickup_datetime,
        tpep_dropoff_datetime  as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee
    from
        filtered
),

-- there is cancelation-trip where total_amount is negative, fare=0
indexed as (
    select
        md5(
            cast(vendor_id as varchar) || '|' ||
            cast(pickup_datetime as varchar) || '|' ||
            cast(dropoff_datetime as varchar) || '|' ||
            cast(pickup_location_id as varchar) || '|' ||
            cast(dropoff_location_id as varchar) || '|' ||
            cast(passenger_count as varchar) || '|' ||
            cast(trip_distance as varchar) || '|' ||
            cast(fare_amount as varchar) || '|' ||
            cast(total_amount as varchar) -- distinct cancellation trip to the trip that is canceled.
        ) as trip_id,
        *
    from
        renamed
)

select * from indexed
  );
