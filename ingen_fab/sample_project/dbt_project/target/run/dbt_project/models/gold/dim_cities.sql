
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_gold.dim_cities as DBT_INTERNAL_DEST
      using dim_cities__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.CityID = DBT_INTERNAL_DEST.CityID
          

      when matched then update set
         * 

      when not matched then insert *
