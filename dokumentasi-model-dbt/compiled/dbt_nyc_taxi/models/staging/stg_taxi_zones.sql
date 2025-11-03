

with
source_data as (
    select
        *
    from
        "dwib"."raw"."taxi_zone_lookup"
    -- bila pakai dbt seed  "dwib"."analytics"."taxi_zone_lookup"
),

-- LocationID        Borough                     Zone service_zone
-- ke lower case
renamed as (
    select
        locationid as location_id,
        lower(borough)      as borough,
        lower(zone)         as zone,
        lower(service_zone) as service_zone

        -- bila pakai dbt seed, nama kolom masih asli
        -- "LocationID" as location_id,
        -- lower("Borough")      as borough,
        -- lower("Zone")         as zone,
        -- lower("service_zone") as service_zone
    from
        source_data
)

select * from renamed