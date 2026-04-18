
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select area_name
from "housing_warehouse"."analytics"."mart_london_affordability_monthly"
where area_name is null



  
  
      
    ) dbt_internal_test