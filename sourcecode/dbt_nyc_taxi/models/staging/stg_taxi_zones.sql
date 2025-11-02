{{
    config(
        materialized = "view",
        schema = "staging"
    )
}}

with
source_data as (
    select
        *
    from
        {{ source('raw', 'taxi_zone_lookup') }}
),

-- LocationID        Borough                     Zone service_zone
-- ke lower case
renamed as (
    select
        locationid as location_id,
        lower(borough)      as borough,
        lower(zone)         as zone,
        lower(service_zone) as service_zone
    from
        source_data
)

select * from renamed