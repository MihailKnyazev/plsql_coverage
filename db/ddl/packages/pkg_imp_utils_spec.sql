CREATE OR REPLACE PACKAGE PKG_IMP_UTILS 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
    /**
     * Common utility functions and procedures whic is used in pkg_ext_imp
     * and pkg_imp_run packages
     */
as
    v_is_incremental number(1) := 0;

    function is_incremental return number;

    /**
     * Check if p_value has value, i.e. not null and not empty string
     *
     * @return true - if p_value null or empty string, false otherwise
     */
    function is_empty_str(p_value in varchar2) return boolean;

    /**
     * Create default import using pkg_ext_imp.XitorConfiguredFieldLoad.
     * Generate entity search sql based on relation_type.unique_by_xt_id value
     *
     * @param p_rtid <code>relation_type.relation_type</code> import will
     *        be created for child xitor type of specified relation type
     * @param p_name name of new import
     * @return <code>imp_spec.imp_spec_id</code>
     */
    function create_def_import(
        p_rtid in relation_type.relation_type_id%type,
        p_name in imp_spec.name%type)
        return imp_spec.imp_spec_id%type;

    /**
     * Prepare delta data for incremental import
     */
    procedure prepare_imp_delta(p_rid imp_run.imp_run_id%type);

    /**
     * Updates imp_run.entity_pk_count column by counting on imp_run_entity_pk table.
     * Executed in autonomous transaction
     */
    procedure set_entity_pk_count(p_rid imp_run.imp_run_id%type);

    /**
     * Updates imp_run.search_pk_finished_ts column with current_date value.
     * Executed in autonomous transaction
     */
    procedure set_fill_entity_pks_finish_ts(p_rid imp_run.imp_run_id%type);

    /**
     * Case insensitive search for column number by column name
     *
     * @return null when no data found
     */
    function get_col_num(
        p_rid imp_run.imp_run_id%type,
        p_colname imp_column.name%type)
        return imp_run_grid_incr.col_num%type;

    /**
     * Return data from imp_run_grid table
     * @p_rid      import run id
     * @p_colname  column name from file
     * @p_row_num  row number from file
     *
     * @return     value for imp_run_grid_incr.data
     */
    function get_col_data(
        p_rid imp_run.imp_run_id%type,
        p_colname imp_column.name%type,
        p_row_num imp_run_grid_incr.row_num%type)
    return imp_run_grid_incr.data%type;

    /**
     * Convert values usually used for chackbox fields to number (0 or 1).
     *
     * @return 1 for the following values (case insensitive):
     * 'Y', 'YES', '1', 'ALL', 'TRUE'. 0 for all other values
     */
    function convert_boolean(p_bool_value in varchar2) return number;

    /**
     * Return column name from imp_run_grid table by given column number
     * @p_rid      import run id
     * @p_col_num  column number
     *
     * @return     column name
     */    
    function get_col_name(
        p_rid in imp_run.imp_run_id%type,
        p_col_num in imp_run_grid_incr.col_num%type) return varchar2;
end pkg_imp_utils;
/