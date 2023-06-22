CREATE OR REPLACE PACKAGE PKG_FIMP_RUN 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as             
   /**
    * Search xitor ids for CSV rows and store in IMP_RUN_ENTITY_PK,
    * this is experimental performance features. 
    *
    * @param p_cur_imp_run the import run cursor declared in pkg_imp_run
    */
    procedure fill_single_entity_pks(
        p_cur_imp_run in pkg_imp_run.cur_imp_run%rowtype,
        p_imp_entity in pkg_imp_run.cur_imp_entity%rowtype,
        p_is_compare in boolean); --TODO remove with Imp-121196 

   /**
    * Function returns true for data compare if the system parameter is enabled and Use experimental performance features= 0 , otherwise false.
    */    
    function is_compare return boolean; --TODO remove with Imp-121196

   /**
    * This procedure save data that is different in old and new imports
    *
    * @param p_imp_run_id the import id
    */  
    procedure save_diff_data(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type); --TODO remove with Imp-121196 
end pkg_fimp_run;
/