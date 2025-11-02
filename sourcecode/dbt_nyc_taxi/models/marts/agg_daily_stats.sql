{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

select * from {{ ref('int_daily_metrics') }}
order by trip_date
