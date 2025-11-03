
  
  create view "dwib"."analytics_staging"."staging_taxi_trips__dbt_tmp" as (
    

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
        vendorid,
        tpep_pickup_datetime   as pickup_datetime,
        tpep_dropoff_datetime  as dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid,
        dolocationid,
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
)

select * from renamed
  );
