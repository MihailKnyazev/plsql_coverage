CREATE OR REPLACE PACKAGE PKG_EXT_IMP 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */

/**
 * This package contains external import procedures
 */
as

    -- type specification to import separate label of language using ConfiguredVTableLoad import
    type r_lang is record(
        label_text  label_program.label_program_text%type,
        app_lang_id app_languages.app_lang_id%type);

    type t_lang_array is table of r_lang;


    /**
     * Deprecated
     * This import procedure is deprecated, please use "ConfiguredField" instead.
     * Imports configured fields. Expected header:
     * XITOR_TYPE,CONFIG_FIELD_NAME,APP_LABEL_TEXT,DESCRIPTION,DATA_TYPE,FIELD_SIZE,
     * FIELD_WIDTH,LINES_QTY,IS_READ_ONLY,TABLE_NAME,
     * IS_LOCKABLE,IS_MANDATORY,IS_TWO_COLS_SPAN,LOG_BLOB_CHANGES,SQL_QUERY,
     * USE_IN_MY_THINGS_FILTER,DELETE
     * <p />
     * Note: XITOR_TYPE - is name of target xitor type,
     * DATA_TYPE is one of: TEXT, NUMBER, DATE, CHECKBOX, DROP-DOWN MENU
     * (or DROP-DOWN or DROPDOWN), MEMO, MULTISELECTOR (or ISELECTOR)
     * If DELETE one of 'YES', 'Y', 'X', '1', 'DEL', 'DELETE' field will be
     * deleted from db
     * If field with same name for same xitor type already exists it will be
     * updated otherwise creates new
     * <p />
     * Also it can handle 2 line headers generated with CSV export on
     * "Admin Config Fields" page (1st line containing labels will be ignored)
     *
     * @param rid the <code>imp_run_id</code> import run ID
     */
    procedure ConfiguredFieldLoad(rid imp_run.imp_run_id%type);

    /**
     * Import security role privileges. Following columns must be presented in
     * import data source (csv file): ROLE_TYPE, SECURITY_GROUP, PRIV.
     * Columns are searched by name, column order doen't matter.
     *
     * @param rid the <code>imp_run_id</code> import run ID
     */
    procedure SecRoleLoad(rid in imp_run.imp_run_id%type);


    /**
     * Load dates to a task at the xitor level or to a task at the sub-xitor
     * level based on the value of a configured field (for example, "Primary"
     * candidates for sar workplans, candidate rank configured field id would
     * be passed as p_cfid and "Primary" would be passed as p_cf_value)
     * or sub xitor key from CSV (column number of sub-xitor key would be passed
     * as p_sub_xitor_key_col_num). Dates are either for the sar level or
     * for the sub-xitor.
     *
     * When there are no sub-xitors for wp's then xitor taks with xitor_id = null
     * will be updated
     *
     * Imports Files of CSV format Xitor Key, WorkPlan Name, then list of columns
     * with the name made of a compound of B,P,A,or R for Baseline, Projected,
     * Actual or pRomised, then S or F Start or Finish, or Date Pair Abbr Label
     * and then an Order Number.  The column name should look like this "PF123" for
     * Projected Finish Task Order Number 123. To set task N/A attribute use
     * column name in following format NA123, where 123 is Task Order Number,
     * values in this column is one of 'Yes', '1', 'Y' (in any case) to set N/A,
     * 'NO', 'N', '0' to unset N/A or leave cell blank to keep existing value.
     * Same works for BlockCalc, but col name must be BlockCalc123
     *
     * Note: if p_sub_xitor_key_col_num is not null, sub-xitor key from csv will
     * be used, set p_sub_xitor_key_col_num = null and p_cfid, p_cf_value
     * not null if you want sub-xitor to be searched by value
     *
     * When p_sub_xitor_key_col_num, p_cfid and p_cf_val are null import will
     * update all tasks of xitor
     *
     * @param p_rid the <code>imp_run_id</code> import run ID
     * @param p_xt_id xitor type id of xitor, which tasks to update
     * @param p_sub_xitor_key_col_num column number which contain sub-xitor keys,
     *        starting from 1 (note columns 1 and 2 are reserved for xitor key
     *        and wp name)
     * @param p_cfid id of config field which will be used to search sub-xitors,
     *        which tasks to update
     * @param p_cf_value value of config field
     * @param p_calc_dates when 1 pkg_wp.update_task will be used to update
     *                           'PS', 'PF', 'AS', 'AF' dates with allowed propagation
     *                     when 2 pkg_wp.update_task will be used to update
     *                           'PS', 'PF', 'AS', 'AF' dates with prohibited propagation
     *                     otherwise pkg_dl_support.set_date will be used
     */
    procedure WpDatesByOrderNum(
        p_rid imp_run.imp_run_id%type,
        p_xt_id xitor_type.xitor_type_id%type,
        p_sub_xitor_key_col_num number default null,
        p_cfid config_field.config_field_id%type default null,
        p_cf_value varchar2 default null,
        p_calc_dates number default 0);


    /**
     * Same as WpDatesByOrderNum but uses Configured field mappings to serach workplan_id
     */
    procedure WpDatesByOrderNumComplex(
        p_rid imp_run.imp_run_id%type,
        p_sub_xitor_key_col_num number default null,
        p_cfid config_field.config_field_id%type default null,
        p_cf_value varchar2 default null,
        p_calc_dates number default 0);

    /**
     * Import configured field values. To find records which will be updated
     * it uses same mechanism as regular import does, so you need to create
     * mappings for each xitor type and enter sql request on form 380801, you
     * can use values from csv or special values ('Program ID', 'Client ID', 'User ID')
     * in parameters just like with regular config imports. Fields used
     * in search sql should be specified on "Fields" tab (380202), this fields
     * will be ignored during import, but you can specify mapping for this
     * fields manually (see below). Also it can create
     * xitors if "Insert" or "Insert/Update" action selected.
     * <br />
     * Following header is expected in csv source:
     * <br />
     * LINE_NUM,CONFIG_FIELD_NAME1,CONFIG_FIELD_NAME2,CONFIG_FIELD_NAME3,...
     * where CONFIG_FIELD_NAMEX is name or label of configured field to import
     * (field must already exists in CONFIG_FIELD table)
     * LINE_NUM - line number
     * <p />
     * Also it can handle 2 line headers generated with CSV export on
     * "Trackor Browser" page (1st line containing labels will be ignored)
     * <p .>
     * To import data for config fields of static xitor types
     * (xitor_type.is_static_definition = 1) specify xitor_type_id in p_xt_id
     * argument, but in this case all config fields to import must be of this xitor
     * type.
     * <p />
     * Also ensure that sql query you specified on 380801 form will not return
     * id for xitor types to which is not belongs, i.e. if you use query like following:
     * <code>select xitor_id into :VALUE from xitor where xitor_key = ':x';</code>
     * for more then one xitor type in single import, it can return id for all
     * rows, regardless it's type, so in general you should use queries like following:
     * <code>select xitor_id into :VALUE from xitor where xitor_key = ':x' and xitor_type_id = 1;</code>
     * <p />
     * You can specify data mapping for some columns as for regular imports,
     * in this case data from this columns will be imported via regular import,
     * all other columns will be imported via external import.
     * <p />
     * Columns order doesn't matter.
     *
     * @param rid the <code>imp_run_id</code> import run ID
     * @param p_xt_id the <code>xitor_type_id</code>
     * @param p_is_search_cf_by_label when 1 import will additionally search CF by label or label with trackor type prefix, 
     *        using language of user who started the import. Search by label text is case insensitive
     * @param p_log_error when 1 store import error in Config Field comment
     */
    procedure XitorConfiguredFieldLoad(
        rid in imp_run.imp_run_id%type,
        p_xt_id in xitor_type.xitor_type_id%type default null,
        p_is_search_cf_by_label in number default 0,
        p_log_error in number default 0);


   /**
    *   Create/update OneVizion user accounts, user Trackor records, and user settings
    * <p />
    *   Expected header:
    * <br />
    *   UN,EMAIL,IS_DISABLED,MUST_CHANGE_PASSWORD,IS_SUPERUSER,
    *   APPLICATION(or Menu),IS_EXTERNAL_LOGIN,IS_ADD_QUOTE_DELIM,
    *   CHECKBOX_MODE(or CHECKBOX_MODE_ID),IS_AUTO_SAVE_TB_GRID_CHANGES,
    *   COORDINATE_MODE(or COORDINATE_MODE_ID),DATE_FORMAT,DEFAULT_PAGE(or DEFAULT_PAGE_ID),
    *   IS_EXACT_SEARCH_CLIPBOARD,FV_LIST_MODE(or FV_LIST_MODE_ID),
    *   IS_COMMENTS_ON_MOUSE_OVER,MFA_TYPE,IS_SHOW_USER_CHAT_STATE_IN_GRID
    * <br />
    *   GRID_EDIT_MODE(or GRID_EDIT_MODE_ID),
    *   IS_HIDE_START_DATE,IS_MUTE_NEW_EVENTS,
    *   APP_LANG_NAME(or APP_LANG_ID),
    *   IS_SHOW_TIP_OF_THE_DAY,THOUSANDS_SEPARATOR,
    *   TIME_FORMAT,OWNER_XITOR_TYPE,
    *   MAXIMIZE_NEW_WINDOW,LINKED_VALUES_DISP_MODE,
    *   IS_CASE_SENSITIVE_SORTING,IS_HIDE_FIELD_TAB_PREFIX,IS_MAIN_MENU_STICKY,PHONE_NUMBER,
    *   Security Role 1,Security Role 2,Security Role 3, SECURITY_ROLES,
    *   Global Views,Global Filters,
    *   Discipline 1,Discipline 2,Discipline 3
    * <p />
    *   Note: UN - is a name of a user;
    * <br />
    *   Security Role - is a name of a security role;
    * <br />
    *   APPLICATION (or APPLICATION_ID or Menu) - is a name of an application menu;
    * <br />
    *   Global Views - is a boolean parameter to assign global views to a user, it has to be as '0' or '1';
    * <br />
    *   Global Filters - is a boolean parameter to assign global filters to a user, it has to be as '0' or '1';
    * <br />
    *   Discipline - is a name of a discipline.
    * <br />
    *   OWNER_XITOR_TYPE - is a name of owner trackor type for user.
    * <p />
    *   Supports import of Config Field values to the User Trackor
    * <br />
    *   Supports relations creation between User and parent Trackor Records
    * <br />
    *   You have to use Trackor Type Name as Excel column name to create relation between this trackor type and "USER" trackor type
    * <p />
    *   To create template file for impor you can use the export data from "Administer account" page and "User" trackor type page
    *
    *   @param p_rid           - Import run id
    *   @param p_update_xitors - Load or not fields, which presente in User trackor type 
    *   @param p_user_xt_id    - User Trackor Type id 
    *   @param p_key_col_name  - Key field name for User trackor Type
    */
    procedure UsersLoad(p_rid in imp_run.imp_run_id%type,
        p_update_xitors in number default 0,
        p_user_xt_id in xitor_type.xitor_type_id%type default null,
        p_key_col_name in varchar2 default null);

    /**
     * This is marker procedure, indicating that data should be parsed from CSV
     * and stored in imp_run_grid for future use. If there are mappings on config import
     * it will be executed
     */
    procedure parse_csv(p_rid in imp_run.imp_run_id%type);


    /**
     * Import which can be run from Trackor Browser page.
     * It uses IMP_RUN.SELF_IMP_COLS to limit columns which will be imported.
     * Addiotionally xitor_ids for all Trackor Types presented in csv should
     * be stored in columns with names of following format Txt_id_XITOR_ID
     * where xt_id is xitor_type_id, xitor_ids of the primary Trackor Type
     * should be first in Txt_id_XITOR_ID columns.
     */
--    procedure self_service_imp(p_rid in imp_run.imp_run_id%type);


    /**
     * ConfiguredVTableLoad inserts or updates Configured V Table value for a given VTable.
     * <br />
     * Import supports all operations, such as insert, update, insert / update
     * <br />
     * Required columns TABLE_NAME, VALUE, ORDER_NUMBER (or ORDER_NUM).
     * <br />
     * Optional supported columns: ATTRIB_V_TABLE_VALUE_ID, DISPLAY, COLOR
     * <br />
	 * To fill VTable value labels add additional columns using Language as column header. This way you may set labels for multiple languages using own column for each.
     * <br />
     * Below is a description of each import operation:
     * <br />
     * Insert: Only insert new values and labels (if these values are presented in the table then will generate an error)
     * <br />
     * Update/Insert: If the value for the ATTRIB_V_TABLE_VALUE_ID column is not null then update ORDER_NUMBER, VALUE, DISPLAY, COLOR and labels.     
     * <br />
     *                If the value for the ATTRIB_V_TABLE_VALUE_ID column is null or 0 or this column is missing from the import
     * <br />
     *                then we finding record in DB using VALUE field. 
     * <br />
     *                If it's find then update only ORDER_NUMBER, DISPLAY and COLOR.
     * <br />
     *                If it isn't find then create new record with imported VALUE, ORDER_NUMBER, DISPLAY, COLOR and labels.
     * <br />
     * Update: If the value for the ATTRIB_V_TABLE_VALUE_ID column is not null then update VALUE, ORDER_NUMBER, DISPLAY, COLOR  and labels
     * <br />
     *         If the value for the ATTRIB_V_TABLE_VALUE_ID column is null or 0 or this column is missing from the import then update only ORDER_NUMBER, DISPLAY by VALUE.
     * <br /> 
     *
     * @param p_rid the <code>imp_run_id</code> import run ID
     */
    procedure ConfiguredVTableLoad(p_rid in imp_run.imp_run_id%type);

    /**
     * Import config field values and wp task dates from Excel with VBA
     */
    procedure CfTaskDateExcelSubmit(p_rid in imp_run.imp_run_id%type);


    /**
     * Imports configured fields. Expected header:
     * TRACKOR_TYPE,CONFIG_FIELD_NAME,CONFIG_FIELD_ID,LABEL_TEXT,DESCRIPTION,DATA_TYPE,FIELD_SIZE,
     * FIELD_WIDTH,LINES_QTY,IS_READ_ONLY,TABLE_NAME,
     * IS_MASKABLE,IS_LOCKABLE,IS_MANDATORY,IS_TWO_COLS_SPAN,LOG_BLOB_CHANGES,
     * SQL_QUERY,USE_IN_MY_THINGS_FILTER,DELETE,SHOW_EXPANDED
     * Required columns are: TRACKOR_TYPE,LABEL_TEXT,DATA_TYPE
     *
     * <p />
     * Note, the import of Config fields obeys the next rules:
     * If CONFIG_FIELD_NAME is null and LABEL_TEXT is not null and CONFIG_FIELD_ID is null 
     * then New Config Field will be created and CONFIG_FIELD_NAME will be generated for the new field from Trackor Type prefix "_" LABEL_TEXT and the rest of the fields will be imported
     *
     * If CONFIG_FIELD_NAME is null and LABEL_TEXT is not null and CONFIG_FIELD_ID is not null 
     * then No action for CONFIG_FIELD_NAME for the existing CONFIG_FIELD_ID(because we can't update it to null) and the rest of the fields will be updated.
     *
     * If CONFIG_FIELD_NAME is not null and LABEL_TEXT is not null and CONFIG_FIELD_ID is not null 
     * then CONFIG_FIELD_NAME will be updated for the existing CONFIG_FIELD_ID from CONFIG_FIELD_NAME and the rest of the fields will be updated
     *
     * If CONFIG_FIELD_NAME is not null and LABEL_TEXT is not null and CONFIG_FIELD_ID is null and Config Field wasn't found using CONFIG_FIELD_NAME 
     * then New Config Field will be created from CONFIG_FIELD_NAME and the rest of the fields will be imported, 
     * if Config Field was found then only the rest of the fields will be updated except CONFIG_FIELD_NAME.
     *
     * TRACKOR_TYPE - is name or label of the target Trackor Type,
     *
     * DATA_TYPE is one of: Text, Number, Date, Checkbox, Drop-Down, Memo,
     * Wiki, DB Drop-Down, DB Selector, Selector, Latitude, Longitude, Electronic File,
     * Trackor Selector, Trackor Drop-Down, Calculated, Hyperlink, Text Label, Rollup,
     * MultiSelector, Date/Time, Time.
     *
     * If DELETE one of 'YES', 'Y', 'X', '1', 'DEL', 'DELETE' field will be
     * deleted from db.
     *
     * CONFIG_FIELD_NAME OR CONFIG_FIELD_ID should be not empty for update and delete operations.
     * 
     * <p />
     * If field with same name for same trackor type already exists it will be
     * updated otherwise creates new.
     * <p />
     * Also it can handle 2 line headers generated with CSV export on
     * "Admin Config Fields" page (1st line containing labels will be ignored)
     *
     * @param p_rid the <code>imp_run_id</code> import run ID
     */
    procedure ConfiguredFields(p_rid in imp_run.imp_run_id%type);

    /**
     * Imports configured fields with Tabs. Expected header:
     * TRACKOR_TYPE,CONFIG_FIELD_NAME,CONFIG_FIELD_ID,LABEL_TEXT,DESCRIPTION,DATA_TYPE,FIELD_SIZE,
     * FIELD_WIDTH,LINES_QTY,IS_READ_ONLY,TABLE_NAME,
     * IS_MASKABLE,IS_LOCKABLE,IS_MANDATORY,IS_TWO_COLS_SPAN,LOG_BLOB_CHANGES,
     * SQL_QUERY,USE_IN_MY_THINGS_FILTER,DELETE,SHOW_EXPANDED
     * TAB_TRACKOR_TYPE, TAB_NAME,TAB_LABEL
     * Required columns are: TRACKOR_TYPE,LABEL_TEXT,DATA_TYPE,TAB_NAME
     *
     * <p />
     * Note, the import of Config fields obeys the next rules:
     * If CONFIG_FIELD_NAME is null and LABEL_TEXT is not null and CONFIG_FIELD_ID is null 
     * then New Config Field will be created and CONFIG_FIELD_NAME will be generated for the new field from Trackor Type prefix "_" LABEL_TEXT and the rest of the fields will be imported
     *
     * If CONFIG_FIELD_NAME is null and LABEL_TEXT is not null and CONFIG_FIELD_ID is not null 
     * then No action for CONFIG_FIELD_NAME for the existing CONFIG_FIELD_ID(because we can't update it to null) and the rest of the fields will be updated.
     *
     * If CONFIG_FIELD_NAME is not null and LABEL_TEXT is not null and CONFIG_FIELD_ID is not null 
     * then CONFIG_FIELD_NAME will be updated for the existing CONFIG_FIELD_ID from CONFIG_FIELD_NAME and the rest of the fields will be updated
     *
     * If CONFIG_FIELD_NAME is not null and LABEL_TEXT is not null and CONFIG_FIELD_ID is null and Config Field wasn't found using CONFIG_FIELD_NAME 
     * then New Config Field will be created from CONFIG_FIELD_NAME and the rest of the fields will be imported, 
     * if Config Field was found then only the rest of the fields will be updated except CONFIG_FIELD_NAME.
     *
     * If TAB_NAME is null and TAB_LABEL is null then the import behavior is equal to pkg_ext_imp.ConfiguredFields.
     * If TAB_NAME is null OR TAB_LABEL is null then they will be replaced by the one that is not null.
     * If TAB_TRACKOR_TYPE is not specified then TRACKOR_TYPE will be used. If TAB_TRACKOR_TYPE and TRACKOR_TYPE
     * are different then the possibility of adding a field to the tab will be checked.
     * If TAB is not found, a new TAB will be created and the field added to it, 
     * otherwise the field will be added below other elements of the existing tab.
     *
     * TRACKOR_TYPE - is name or label of the target Trackor Type,
     *
     * DATA_TYPE is one of: Text, Number, Date, Checkbox, Drop-Down, Memo,
     * Wiki, DB Drop-Down, DB Selector, Selector, Latitude, Longitude, Electronic File,
     * Trackor Selector, Trackor Drop-Down, Calculated, Hyperlink, Text Label, Rollup,
     * MultiSelector, Date/Time, Time.
     *
     * If DELETE one of 'YES', 'Y', 'X', '1', 'DEL', 'DELETE' field will be
     * deleted from db.
     *
     * CONFIG_FIELD_NAME OR CONFIG_FIELD_ID should be not empty for update and delete operations.
     * 
     * <p />
     * If field with same name for same trackor type already exists it will be
     * updated otherwise creates new.
     * <p />
     * Also it can handle 2 line headers generated with CSV export on
     * "Admin Config Fields" page (1st line containing labels will be ignored)
     *
     * @param p_rid the <code>imp_run_id</code> import run ID
     */
    procedure ConfiguredFieldsWithTabs(p_rid in imp_run.imp_run_id%type);

   /**
    * Update Program Label Text
    * <p />
    *   Expected header:
    * <br />
    *   Label ID (or any other name, but needs to be the first in the file),
    *   *additional columns with Language Name in a header
    * <br />
    *  *To fill label text value for separate language add additional columns using Language as column header. 
    *   This way you may set labels for multiple languages using own column for each.
    * <br />
    *   @param p_rid - Import run id
    */
    procedure label_program_load(p_rid in imp_run.imp_run_id%type);

   /**
    * Update System Label Text
    * <p />
    *   Expected header:
    * <br />
    *   Label ID (or any other name, but needs to be the first in the file),
    *   *additional columns with Language Name in a header
    * <br />
    *  *To fill label text value for separate language add additional columns using Language as column header. 
    *   This way you may set labels for multiple languages using own column for each.
    * <br />
    *   @param p_rid - Import run id
    */
    procedure label_system_load(p_rid in imp_run.imp_run_id%type);

   /**
    * Update Task Label Text
    * <p />
    *   Expected header:
    * <br />
    *   Label ID (or any other name, but needs to be the first in the file),
    *   *additional columns with Language Name in a header
    * <br />
    *  *To fill label text value for separate language add additional columns using Language as column header. 
    *   This way you may set labels for multiple languages using own column for each.
    * <br />
    *   @param p_rid - Import run id
    */
    procedure label_task_load(p_rid in imp_run.imp_run_id%type);

    /**
     * Function safely convert string to number
     *
     * @param p_value -  String value
     * @return  - When no errors then return p_value as number, when error - return null
     */
    function char_to_number(p_value in varchar2) return number deterministic;
end pkg_ext_imp;
/