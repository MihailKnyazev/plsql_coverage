CREATE OR REPLACE PACKAGE PKG_IMP_RUN 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */

  /**
   * This package executes all configured imports, but external.
   */
as

  --Error (warining) type constants
  c_et_pk constant number := 1;
  c_et_data constant number := 2;
  c_et_conf_field constant number := 3;
  c_et_req_val constant number := 4;
  c_et_val constant number := 5;
  c_et_new_xitor constant number := 6;
  c_et_ext_not_found constant number := 7;
  c_et_autokey constant number := 8;
  c_et_wp_rule constant number := 9;
  c_extimp constant number := 10;
  c_et_unknown constant number := 11;
  c_et_new_xitor_rule constant number := 12;
  c_et_update_xitor_rule constant number := 13;
  c_et_del_xitor constant number := 14;
  c_et_xitor_exists constant number := 15;
  c_et_cant_set_na constant number := 16;
  c_et_imp_conf constant number := 17;
  c_et_imp_complete_rule constant number := 18;
  c_et_imp_start_rule constant number := 19;
  c_et_new_user_rule constant number := 20;
  c_et_update_user_rule constant number := 21;
  c_et_imp_data_validation_error constant number := 22;
  c_et_invalid_sql constant number := 23;
  c_et_must_be_sql constant number := 24;
  c_et_create_update_xitor_rule constant number := 25;

  --Status constants
  c_s_pending constant number := 0;
  c_s_imp_data constant number := 5;
  c_s_end constant number := 6;
  c_s_end_errs constant number := 7;
  c_s_parse constant number := 8;
  c_s_pk constant number := 9;
  c_s_gen_sql constant number := 10;
  c_s_interrupted constant number := 14;
  c_s_rules_imp_start constant number := 16;
  c_s_rules_imp_finish constant number := 17;

    c_imp_action_update constant imp_action.imp_action_id%type := 2;
    c_imp_action_insert_update constant imp_action.imp_action_id%type := 3;
    c_imp_action_insert constant imp_action.imp_action_id%type := 4;

  /**
   * Oracle directory to be used for temporary file storage with fill_grid_loader procedure
   */
  c_loader_ora_dir constant varchar2(50) := 'DATAPUMP';

  /**
   * Physical path on DB server to the directory specified in c_loader_ora_dir
   */
  c_loader_physical_dir constant varchar2(50) := '/datapump';

  /**
   * Name of the job to be executed from fill_grid_loader to fix inline line breaks
   */
  c_loader_lbfix_job_name constant varchar2(30) := 'CSV_FILE_LB_FIX_JOB';

  /**
   * Owner of job "loader_lbfix_job_name"
   */
  c_loader_lbfix_owner constant varchar2(30) := 'VQADMIN';

  c_auto_generation varchar2(100) := 'AUTO_GENERATION';
  c_trackor_key varchar2(100) := 'TRACKOR_KEY';

  allow_import_mods boolean := false;

  gv_is_dropgrid_deleting boolean := false;

  e_skip_row exception;

    cursor cur_imp_entity(p_spec_id in imp_spec.imp_spec_id%type) is
        select imp_entity_id,
               imp_spec_id,
               sql_text,
               order_number,
               xitor_type_id,
               entity_name,
               program_id
          from imp_entity
         where imp_spec_id = p_spec_id
         order by order_number;

  cursor cur_imp_run(v_id imp_run.imp_run_id%type) is
    select r.imp_run_id,
           p.user_id,
           p.program_id,
           date_format,
           imp_action_id,
           s.imp_spec_id,
           s.use_fimport,
           r.start_row
      from imp_run r
      join imp_spec s on r.imp_spec_id = s.imp_spec_id
      join process p on p.process_id = r.process_id
     where r.imp_run_id = v_id;

  cursor cur_imp_spec(p_rid imp_run.imp_run_id%type) is
    select i.name,
           p.user_id,
           p.program_id,
           p.start_date,
           ir.imp_spec_id, 
           i.date_format,
           i.time_format,
           ir.imp_run_id,
           ir.notify_on_completion,
           p.process_id,
           i.days_to_keep_parsed_data,
           ir.start_row
    from process p, imp_run ir, imp_spec i
    where p.process_id = ir.process_id and i.imp_spec_id = ir.imp_spec_id and ir.imp_run_id = p_rid;

  procedure no_direct_import_mods;

  function delete_import(p_imp_spec_id imp_spec.imp_spec_id%type) return number;

  procedure delete_import(p_imp_spec_id imp_spec.imp_spec_id%type);


  /**
   * Main entry point to package, both for regular and external imports
   *
   * @param p_proc_id the <code>process_id</code> number
   */
  procedure import(p_proc_id process.process_id%type);


  /**
   * Main entry point to package, both for regular and external imports
   *
   * @param p_proc_id the <code>process_id</code> number
   */
  procedure incremental_import(
    p_proc_id process.process_id%type,
    p_is_incremental imp_run.is_incremental%type);

  /**
   * Utility procedure to log error (warning) during import. Writes values into the
   * <code>imp_run_error</code> table.
   *
   * @param p_rid the <code>imp_run_id</code> id of import
   * @param p_msg a string value of an error description
   * @param p_err_type_id - error type, constants (c_et_ prefix) must be used
   * @param p_row_num the row number in source csv where error occurs
   * @param p_sql the SQL text which generates the said error message
   * @param p_dmid the <code>data_map_id</code>
   * @param p_xid the <code>xitor_id</code>
   * @param p_col_name name of importing column
   * @param p_bad_data_value bad value which caused error
   */
  procedure write_error(
    p_rid in imp_run_error.imp_run_id%type,
    p_msg in clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_sql in imp_run_error.sql_text%type default null,
    p_dmid in imp_run_error.imp_data_map_id%type default null,
    p_entity_id in imp_run_error.entity_id%type default null,
    p_col_name in imp_run_error.col_name%type default null,
    p_bad_data_value in imp_run_error.bad_data_value%type default null);

  /**
   * Set count of imported rows. This procedure called inside import
   * loop after importing each record.
   *
   * @param rid a <code>imp_run_id</code> id of import
   * @param num sets <code>rows_processed</code> equal to this value
   */
  procedure set_rows(rid number, num number);


  /**
   * Return column's order number in source CSV by it's ID
   *
   * @param rid the <code>imp_run_id</code> id of import
   *        order number
   * @param cid the <code>imp_column_id</code> number to locate the column's
   *        order number
   * @return the column's order number
   */
  function col_num(
    rid imp_run.imp_run_id%type,
    cid imp_column.imp_column_id%type) return imp_run_grid.col_num%type;

  /**
   * Return value of cell in CSV file using it's row and column order numbers.
   * This procedure is preferable to cell_value function value, because it returns
   * value in nocopy parameter, especially when large value is expected.
   *
   * @param rid the <code>imp_run_id</code> id of import
   * @param rn the row order number
   * @param cn the column order number
   * @param val the value of the cell as a nocopy parameter
   */
  procedure cell_value(
    rid imp_run.imp_run_id%type,
    rn  imp_run_grid.row_num%type,
    cn  imp_run_grid.col_num%type,
    val in out nocopy clob);

  function cell_value(
    rid imp_run.imp_run_id%type,
    rn imp_run_grid.row_num%type,
    cn imp_run_grid.col_num%type)
    return imp_run_grid.data%type;

  /**
   * Clear <code>IMP_RUN_GRID</code> table, which contains primary keys of
   * created entities or entities to update
   *
   * @param rid the <code>imp_run_id</code> id of import
   */
  procedure drop_pks(rid number);

  /**
   * Add primary key of created entity or entity to update to
   * <code>IMP_RUN_GRID</code> table
   *
   * @param rid the <code>imp_run_id</code> id of import
   * @param eid the <code>imp_entity_id</code>
   * @param rnum the <code>row_num</code> row number in source CSV
   *        corresponding to added key
   * @param pkid the <code>pk</code> primary key to be added
   * @param v_inserted the <code>is_inserted</code>, must be 1 when new
   *        entity (xitor) created, or 0 when pk was found for entity
   *        (xitor) to update
   */
  procedure add_pk(
    rid number,
    eid number,
    rnum number,
    pkid number,
    v_inserted number);

  /**
   * Builds sql to find entity to update
   *
   * @param pksql the built sql no copy parameter
   * @param v_entid the <code>imp_entity</code> number to identify the entity
   *        to update
   * @param v_row_num a row order number used to find a cell value
   * @param v_imp_run a <code>cur_imp_run</code> list of rows used to find a cell
   *        value and help build sql
   */
  procedure buld_update_ent_sql(
    pksql in out nocopy clob,
    v_entid in imp_entity.imp_entity_id%type,
    v_row_num in number,
    v_imp_run in cur_imp_run%rowtype);

  /**
   * Utility procedure to execute "New Xitor" and "Update Xitor" triggers.
   * It uses IMP_RUN_ENTITY_PK to find xitors and IS_INSERTED to determine
   * which xitors are new and which are updated
   *
   * @param p_rid the <code>imp_run_id</code> import id
   */
  procedure exec_xitor_triggers(p_rid imp_run.imp_run_id%type);

  /**
   * Search xitor ids for CSV rows and store in IMP_RUN_ENTITY_PK.
   */
    procedure fill_entity_pks(
        p_process_id in process.process_id%type,
        p_imp_run_id in imp_run.imp_run_id%type);


  /**
   * Actually import data, This procedure must be called after csv already
   * parsed (<code>fill_grid</code>), pks to update are found
   * (<code>fill_entity_pks</code>) and data maps are prepared
   * (<code>fill_datamap_sql</code>)
   */
  procedure import_data(pr_spec in cur_imp_spec%rowtype);

  /**
   * Fill <code>imp_run_data_map_sql</code> table
   */
  procedure fill_datamap_sql(pr_spec in cur_imp_spec%rowtype);

  /**
   * Generate xitor_key for xitor types with key autogeneration. Must be
   * executed after all data imported, because key generation can be based
   * on config field values
   */
  procedure generate_xitor_keys(p_rid imp_run.imp_run_id%type);

  procedure notify(p_rid in imp_run.imp_run_id%type);

  /**
   * Deletes rows where all columns (except specified in comma separated list in p_col_nums_to_skip)
   * has null values, keeps row_num sequential
   */
  procedure del_empty_grid_rows(
    p_rid in imp_run.imp_run_id%type,
    p_col_nums_to_skip in varchar2 default null);

  /**
   * Fill imp_run_grid using SQL Loader (much faster then fill_grid)
   */
  --    function loader_fill_grid(
  --        p_rid imp_run.imp_run_id%type,
  --        p_update_imp_status in boolean default true)
  --        return number;

  procedure set_status(
    p_proc_id process.process_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_status_id process.status_id%type);

  /**
   * Utility function to decode delimiter string from '[x][y]...' format
   * (where x and y are ASCII symbol codes) to actual symbols.
   *
   * @param arg_delim the delimiter string which one wishes to decode. Must be
   *        in this format: '[x][y]...'
   * @return the decoded delimiter string from the '[x][y]...' format. For example
   *         if you passed in '[51][80][43]' it would return the string '3P+'.
   */
  function decode_delim_str(arg_delim in varchar2)
    return varchar2;

  function get_excell_col_name(p_col_num in number) return varchar2;

  function is_select_statement(p_sql in varchar2) return boolean;

  function ireplace(srcstr clob, oldsub clob, newsub clob) return clob;

  function build_insert_ent_sql(
      v_xtid in xitor_type.xitor_type_id%type,
      v_entid in imp_entity.imp_entity_id%type,
      v_row_num in number,
      v_imp_run in cur_imp_run%rowtype) return varchar2;

  function run_sql_ret(
      p_rid in imp_run_error.imp_run_id%type,
      p_sql in out nocopy clob,
      p_err_type_id in imp_run_error.imp_error_type_id%type,
      p_row_num in imp_run_error.row_num%type,
      p_dmid in imp_run_error.imp_data_map_id%type default null) return number;

  function run_entity_plsql(p_pksql in out nocopy clob) return list_id; 

  /**
   * Utility procedure to log error (warning) during import. Writes error values into the
   * <code>field_comment</code> table.
   *
   * @param p_rid the <code>imp_run_id</code> id of import
   * @param p_msg a string value of an error description
   * @param p_row_num the row number in source csv where error occurs
   * @param p_field_name the config field name
   * @param p_pk the <code>trackor_id</code> Trackor id
   * @param p_ttid the <code>trackor_type_id</code> Trackor type id
   * @param p_dmid the <code>imp_data_map_id</code> Data mapping id
   * @param p_val Cell value from file
   * @param p_imp_col_num the imported column number. Default null. Used when it is invokes from XitorConfiguredFieldLoad
   * @param p_log_error when 1 - Save errors to field_comments table, default 0. Used when it is invokes from XitorConfiguredFieldLoad
   */
  procedure write_field_comment(
    p_rid in imp_run_error.imp_run_id%type,
    p_msg in field_comment.comments%type,
    p_row_num in imp_run_error.row_num%type,
    p_field_name in field_comment.field_name%type,
    p_pk in field_comment.pk%type,
    p_ttid in field_comment.trackor_type_id%type,
    p_dmid in imp_run_error.imp_data_map_id%type,
    p_val in out nocopy clob,
    p_imp_col_num number default null,
    p_log_error number default 0);

  /**
   * Following types shouldn't be used outside this
   * package. I'm declare it here because use it in
   * dynamic pl/sql block
   */
  type t_delimiters is record(
    fields_delimiter varchar2(100),
    records_delimiter varchar2(100)
  );

  type t_quote_rules is record(
    quote_strings number,
    quote_everything number,
    quote_nulls number,
    quote_symbol varchar2(100)
  );
end pkg_imp_run;
/