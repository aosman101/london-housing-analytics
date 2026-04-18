
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select price_to_earnings_ratio
from "housing_warehouse"."analytics"."mart_london_affordability_monthly"
where price_to_earnings_ratio is null



  
  
      
    ) dbt_internal_test