
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_month
from "housing_warehouse"."analytics"."mart_london_affordability_monthly"
where date_month is null



  
  
      
    ) dbt_internal_test