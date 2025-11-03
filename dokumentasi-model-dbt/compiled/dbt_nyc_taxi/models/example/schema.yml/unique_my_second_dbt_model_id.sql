
    
    

select
    id as unique_field,
    count(*) as n_records

from "dwib"."analytics_seeds"."my_second_dbt_model"
where id is not null
group by id
having count(*) > 1


