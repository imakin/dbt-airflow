{{ config(
    materialized = "view",
    schema = "intermediate"
)}}

-- Daily aggregation with key metrics and moving averages

with enhanced_trips as (
    select * from {{ ref('int_trips_enhanced') }}
),

daily_agg as (
    select
        trip_date,
        count(*) as total_trips,
        round(sum(total_amount), 2) as total_revenue,
        round(avg(total_amount), 2) as avg_revenue_per_trip,
        sum(case when payment_type = 1 then 1 else 0 end) as credit_card_trips,
        sum(case when payment_type = 2 then 1 else 0 end) as cash_trips
    from enhanced_trips
    group by trip_date
),

with_moving_averages as (
    select
        *,
        -- Moving averages (requirement: "moving averages")
        round(avg(total_trips) over (
            order by trip_date 
            rows between 6 preceding and current row
        ), 2) as trips_7day_ma,
        round(avg(total_revenue) over (
            order by trip_date 
            rows between 6 preceding and current row
        ), 2) as revenue_7day_ma
    from daily_agg
)

select * from with_moving_averages
order by trip_date
