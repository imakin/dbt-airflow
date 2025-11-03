
  
    
    

    create  table
      "dwib"."analytics_analytics"."agg_daily_stats__dbt_tmp"
  
    as (
      

select * from "dwib"."analytics_intermediate"."int_daily_metrics"
order by trip_date
    );
  
  