CREATE OR REPLACE PACKAGE PKG_EXT_IMP_UTILS 
/*
 * Copyright 2003-2021 OneVizion, Inc. All rights reserved.
 */
as

    /**
      * Utility function to search v_sec_role.role_type by name
      * (v_sec_role.sec_role_id), if sec role doesn't exists null will be
      * returned and <code>pkg_imp_run.write_error</code> will be executed to
      * log error
      */
    function get_sec_role_id(p_role_type in sec_role.role_type%type,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type)
    return sec_role.sec_role_id%type;

    /**
      * Utility procedure to log "NO_DATA_FOUND" errors. Uses
      * pkg_imp_run.write_error procedure. Generates error message in following
      * format:
      * p_field_name "p_field_value" not found
      *
      * @param p_field_name name of the field which value not found
      * @param p_field_value value which is not found
      * @param p_rid imp_run.imp_run_id, will be passed to pkg_imp_run.write_error
      * @param p_row_num row number in import data source (csv file),
      *        will be passed to pkg_imp_run.write_error
      */
    procedure log_not_found_err(p_field_name in varchar2,
        p_field_value in varchar2,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type);

    /**
      * Utility function to retrive date format for particular import
      *
      * @param p_rid imp_run_id for which date format will be returned
      * @return date format
      */
    function get_date_format(p_rid in imp_run.imp_run_id%type)
    return imp_spec.date_format%type;

    function get_time_format(p_rid in imp_run.imp_run_id%type)
    return imp_spec.time_format%type;

    /**
      * Don't use this procedure for import. This is utility procedure used
      * in pkg_migration
      */
    procedure set_cf_data(cfid number,
        entid number,
        p_date_format in imp_spec.date_format%type,
        p_time_format in imp_spec.time_format%type,
        val in out nocopy clob,
        ln number default 1,
        idval number default 0);

end pkg_ext_imp_utils;
/