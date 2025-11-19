
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into lh_silver.countries as DBT_INTERNAL_DEST
      using countries__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.CountryID = DBT_INTERNAL_DEST.CountryID
          

      when matched then update set
         * 

      when not matched then insert *
