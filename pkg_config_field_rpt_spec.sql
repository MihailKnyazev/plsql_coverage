CREATE OR REPLACE package pkg_config_field_rpt 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */

/**
 * This package is used to help write configured field reports. It mostly relates
 * to finding field values so that they can be used to fill reports.
 */
    authid current_user
is
    c_linked_value_err_msg varchar2(5) := 'Err';
    c_static_linked_val_type_id constant v_linked_value_type.linked_value_type_id%type := 1;

    gv_text_cells_display_limit number := pkg_param_program.gc_default_text_cells_display_limit;

   /**
    * Converts the <code>config_field_text</code> to a <code>config_field_id</code> number
    * and searches through the <code>config_field</code> and <code>config_value</code>
    * tables. Finds where a given <code>config_field_id</code> number and a
    * <code>key_value</code> number line up to find information about the datatype
    * properties of the elements selected in this function's query process.
    *
    * @param p_key_value the <code>key_value</code> number
    * @param p_cf_name the string value of a <code>config_field_name</code>
    * @return A <code>config_value</code> within a field will be of one specific
    *         datatype. Therefore within <code>config_value</code> there is only one datatype
    *         column within each field which IS NOT NULL. If this value should be
    *         a <code>value_date</code> or a <code>value_char</code> then it will simply
    *         return that value. If the value should be a <code>value_number</code>,
    *         then it will return information related to the field names or will return
    *         a description of ' not found!' If <code>value_number</code> should
    *         be a ePM Xitor Selector datatype, then it will check if the number is a
    *         <code>xitor_id</code> number and if not found will return 'Xitor Key not found!'
    *         For calculated fields will return 'Calc field error!' if error occur
    *         during execution of calc field query
    */
   function getValStr (
     p_key_value in number,
     p_cf_name in config_field.config_field_name%type)
     return varchar2;

  /**
    * Searches through the <code>config_field</code> and <code>config_value</code>
    * tables and finds where a given <code>config_field_id</code> number and a
    * <code>key_value</code> number line up to find information about the datatype
    * properties of the elements selected in this function's query process.
    *
    * @param p_key_value the <code>key_value</code> number
    * @param p_cfid the <code>config_field_id</code> number
    * @return A <code>config_value</code> within a field will be of one specific
    *         datatype. Therefore within <code>config_value</code> there is only one datatype
    *         column within each field which IS NOT NULL. If this value should be
    *         a <code>value_date</code> or a <code>value_char</code> then it will simply
    *         return that value. If the value should be a <code>value_number</code>,
    *         then it will return information related to the field names or will return
    *         a description of ' not found!' If <code>value_number</code> should
    *         be a ePM Xitor Selector datatype, then it will check if the number is a
    *         <code>xitor_id</code> number and if not found will return 'Xitor Key not found!'
    *         For calculated fields will return 'Calc field error!' if error occur
    *         during execution of calc field query
    */
   function getValStrByID (
     p_key_value in number,
     p_cfid in config_field.config_field_id%type)
     return varchar2;

   /**
    * Converts the <code>config_field_text</code> to a <code>config_field_id</code> number
    * and searches through the <code>config_field</code>and <code>config_value</code>
    * tables and finds where a given <code>config_field_id</code> number and a
    * <code>key_value</code> number line up to find the desired <code>value_number</code> ID.
    * Also, returns Trackor Key's value if it contains digits only. Returns null for non-numeric Trackor Keys.
    *
    * @param pk the <code>key_value</code> number
    * @param config_field_text the string value of a <code>config_field_name</code>
    * @return the <code>value_number</code> ID number
    */
   function getValNumNL(pk number, config_field_text varchar) return number;

   /**
    * Searches through the <code>config_field</code>and <code>config_value</code>
    * tables and finds where a given <code>config_field_id</code> number and a
    * <code>key_value</code> number line up to find the desired <code>value_number</code> ID.
    * Also, returns Trackor Key's value if it contains digits only. Returns null for non-numeric Trackor Keys.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return the <code>value_number</code> ID number
    */
   function getValNumNLByID(pk in xitor.xitor_id%type, config_fieldid in config_field.config_field_id%type) return number;

   /**
    * Converts the <code>config_field_text</code> to a <code>config_field_id</code> number
    * and searches through the<code>config_value</code> table and finds where a given
    * <code>config_field_id</code> number and a <code>key_value</code> number line
    * up to find the desired <code>value_date</code>.
    *
    * @param pk the <code>key_value</code> number
    * @param config_field_text the string value of a <code>config_field_name</code>
    * @return the <code>value_date</code> date
    */
   function getValDate (pk number, config_field_text varchar2) return date;

   /**
    * Searches through the<code>config_value</code> table and finds where a given
    * <code>config_field_id</code> number and a  <code>key_value</code> number line
    * up to find the desired <code>value_date</code>.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return the <code>value_date</code> date
    */
   function getValDateById (pk number, config_fieldid number) return date;

   /**
    * Searches through the <code>config_value</code> table and finds where a given
    * <code>config_field_id</code> number, <code>key_value</code> number, and a
    * <code>line_number</code> coincide to find the desired <code>value_number</code>.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return the <code>value_number</code>
    */
   function getLineBlobIDByID (pk number, config_fieldid number, Line number) return number;

   /**
    * Searches through the <code>config_value</code> and <code>blob_data</code> tables and finds where a given
    * <code>config_field_id</code> number, <code>key_value</code> number, and a
    * <code>line_number</code> coincide to find the desired <code>filename</code>.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @param Line the <code>line_number</code>
    * @return the string value of a <code>filename</code>
    */
   function getLineFileNameByID (pk number, config_fieldid number, Line number) return varchar2;

   /**
    * Searches through the <code>config_value</code> and <code>blob_data</code> tables and finds where a given
    * <code>config_field_id</code> number, <code>key_value</code> number, and a
    * <code>line_number</code> coincide to find the size of a BLOB data file
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @param Line the <code>line_number</code>
    * @return the size of a <code>filename</code> in bytes
    */
   function getLineFileSizeByID (pk number, config_fieldid number, Line number) return number;

   /**
    * Same as getValStrByID except that for checkbox fields, it returns Yes/No
    *
    */
   function getValStrByIDYN(
     p_key_value in number,
     p_cfid in config_field.config_field_id%type)
     return varchar2;

   /**
    * Return value of memo field truncated at 32000 for safe use in Excell, use
    * getFullValMemoByID if you need full value
    */
   function getValMemo(
     p_key_value number,
     config_field_text varchar2)
     return clob;

   /**
    * Return value of memo field truncated at 32000 for safe use in Excell, use
    * getFullValMemoByID if you need full value
    */
   function getValMemoByID(
     pk in number,
     p_cfid in config_field.config_field_id%type)
     return clob;

   /**
    * Returns limited value of memo field according to TextCellsDisplayLimit param.
    */
   function getLimitedValMemoByID(
     pk in number,
     p_cfid in config_field.config_field_id%type)
     return varchar2;

   /**
    * Returns limited value of text field according to TextCellsDisplayLimit param.
    */
   function getLimitedValTextByID(
     pk in number,
     p_cfid in config_field.config_field_id%type)
     return varchar2;

   /**
    * Returns limited value of memo field according to TextCellsDisplayLimit param.
    */
   function getLimitedValMemo(
     pk in number,
     config_field_text varchar2)
     return varchar2;

   /**
    * Return limited value of memo drill-down field according to TextCellsDisplayLimit param.
    */
   function getLimitedValMemoXSID(
     pk number,
     xs_field_id  number,
     config_fieldid  number)
     return varchar2;


   /**
    * Returns value of memo field. For Excell reports use getValMemoByID
    */
   function getFullValMemoByID(
     pk in number,
     p_cfid in config_field.config_field_id%type)
     return clob;

   /**
    * Returns value of memo field.
    */
   function getFullValMemo(
     pk in number,
     config_field_text varchar2)
     return clob;

   /**
    * Returns full value of Text field. 
    * Use getLimitedValTextByID to get value truncated based on TextCellsDisplayLimit parameter.
    */
  function getFullValTextByID(
      p_pk in config_value_char.key_value%type, 
      p_cfid in config_value_char.config_field_id%type) 
      return config_value_char.value_char%type;

   /**
    * Same as getValStr except that for boolean fields, it returns Yes/No
    *
    * @author Igor
    */
   function getValStrYN (pk number, config_field_text varchar2) return varchar2;

   /**
    * Gets values of fields of a number type, single or multiple.Values of Multiple
    * fields will be returned as semicolon separated.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return a list of <code>value_number</code> values
    *
    * @author Igor
    */
   function getAllValNumByID (pk number, config_fieldid number) return varchar2;

   /**
    * Gets values of fields of a number type, ONLY multiple. Values
    * will be returned as semicolon separated.
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return a list of <code>value_number</code> values
    *
    * @author Igor
    */
   function getAllValNumMultByID (pk number, config_fieldid number) return varchar2;

    /**
     * Gets values of fields of a number type, ONLY multiple. Values
     * will be returned as tableofnum (list of number).
     *
     * @param pk the <code>key_value</code> number
     * @param config_fieldid the <code>config_field_id</code> number
     * @return a list of <code>value_number</code> values
     *
     * @author Sergey Kremnev
     */
    function getAllValNumMultByIDasTabOfNum(
        p_key config_value_mult.key_value%type, 
        p_cfid config_value_mult.config_field_id%type) return tableofnum;

   /**
    * Gets numeric values of MultiCelector Drill Down CFs. Values
    * will be returned as semicolon separated.
    *
    * @param pk the <code>key_value</code> of Primary Trackor Type
    * @param p_xs_cfid Drill Down CF (Trackor Selector from Primary Trackor Type)
    * @param p_cfid config_field_id of MultiSelector CF which number values to return
    * @return a list of <code>value_number</code> values
    */
   function getAllValNumMultXS(
     p_pk number,
     p_xs_cfid config_field.config_field_id%type,
     p_cfid config_field.config_field_id%type)
     return varchar2;

   /**
    * Get value of a static field (config_field.is_static=1).
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return a string value which gives information relating to the static fields.
    *         It could say 'Not Used' or it could relay information pertaining to
    *         xitors such as ID numbers, names, and other related information
    *
    * @author Igor
    */
   function getValStrByStaticID (pk number, config_fieldid number) return varchar2;

   /**
    * Get value of a static field (config_field.is_static=1).
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldname the <code>config_field_name</code> number
    * @return a string value which gives information relating to the static fields.
    *         It could say 'Not Used' or it could relay information pertaining to
    *         xitors such as ID numbers, names, and other related information
    *
    * @author Igor
    */
   function getValStrByStaticName (pk number, config_fieldname varchar2) return varchar2;

   /**
    * Return value of v_table for configured fields of Drop-Down, Selector and MultiSelector
    * data type. If v_table or value can't be found it return "[table name] not found!"
    *
    * @param p_vtable_pk pk of v_table's value
    * @param p_cfid
    * @return text value of v_table, or "[table name] not found!" if
    *         table or value can't be found
    */
   function getVTableValueByCfId(
         p_vtable_pk in number,
         p_cfid in config_field.config_field_id%type) return varchar2;

   function getVTableValue(
         p_vtable_pk in number,
         p_table_id in config_field.attrib_v_table_id%type) return varchar2;

    /**
    * Return lable of v_table value for configured fields of Drop-Down, Selector and MultiSelector
    * data type. If v_table or value can't be found it return "[table name] not found!"
    *
    * @param p_vtable_pk pk of v_table's value
    * @param p_cfid
    * @return text label of v_table value, or "[table name] not found!" if
    *         table or value can't be found
    */
   function getVTableLabelByCfId(
         p_vtable_pk in number,
         p_cfid in config_field.config_field_id%type) return label_program.label_program_text%type;

   function getVTableLabel(
         p_vtable_pk in number,
         p_table_id in config_field.attrib_v_table_id%type) return label_program.label_program_text%type;

   /**
    *
    * @param p_is_omit_alias when 1, Trackor Type alias will not be added to they key, even it is set for Trackor Type
    */
    function getTrackorSelectorVal(
        p_key_value in number,
        p_cfid in config_field.config_field_id%type,
        p_obj_xtid in config_field.obj_xitor_type_id%type,
        p_ln in number := null,
        p_is_omit_alias in number default 0)
        return varchar2;

   /**
    *
    * @param p_is_omit_alias when 1, Trackor Type alias will not be added to they key, even it is set for Trackor Type
    */
    function get_tr_selector_val_by_xid(
        p_xid in xitor.xitor_id%type,
        p_cfid in config_field.config_field_id%type,
        p_obj_xtid in config_field.obj_xitor_type_id%type,
        p_is_omit_alias in number default 0)
        return varchar2;

    /**
    *
    * @param p_is_omit_alias when 1, Trackor Type alias will not be added to they key, even it is set for Trackor Type
    */
    function get_trackor_key(
        p_xid in xitor.xitor_id%type,
        p_is_omit_alias in number default 0)
        return varchar2;

   function getCalcSqlValue(
        p_calc_sql in config_field.sql_query%type,
        p_key_value in number,
        p_ln in number := null) return varchar2;

   function getDBSqlValue(
        p_sql_query in config_field.sql_query%type,
        p_id in number,
        p_key_value in number,
        p_ln in number := null)
        return varchar2;

   function getDBSqlId(
        p_cfid in config_field.config_field_id%type,
        p_val in varchar2,
        p_program_id in number)
        return number;

   function getDist(AXitor number, ZXitor number, LatField number, LongField number) return number;

   function getAzimuth(AXitor number, ZXitor number, LatField number, LongField number, direction varchar default 'AZ') return number;

   function getTimeStamp(pk number, config_field_text varchar2) return date;
   function getTimeStampByID(pk number, config_fieldid number) return date;
   function getUserID(pk number, config_field_text varchar2) return number;
   function getUserIDByID(pk number, config_fieldid number) return number;
   function getUserName(pk number, config_field_text varchar2) return varchar2;
   function getUserNameByID(pk number, config_fieldid number) return varchar2;

   function isFieldInUse(config_fieldid number) return number;

   function getValStrStaticXS(pk number, xs_field_id  number,  cf_xt_id  number, config_fieldname varchar2) return varchar2;
   function getValStrStaticXSID(pk number, xs_field_id  number,  cf_xt_id  number) return varchar2;
   function getValStrXS(p_pk number, p_xs_cfid config_field.config_field_id%type, p_cfid config_field.config_field_id%type) return varchar2;
   function getValNumNLXS(p_pk number, p_xs_cfid config_field.config_field_id%type, p_cfid config_field.config_field_id%type) return number;
   function getValDateXS(p_pk number, p_xs_cfid config_field.config_field_id%type, p_cfid config_field.config_field_id%type) return date;
   function getValStrXSID(pk number, xs_field_id  number,  config_fieldid  number) return varchar2;
   function getValStrByParentID(pk in number, cxitor_type_id number, pxitor_type_id number, config_fieldid in config_field.config_field_id%type) return varchar2;
   function getValNumNLByParentID(pk in number, cxitor_type_id number, pxitor_type_id number, config_fieldid in config_field.config_field_id%type) return number;
   function getValXSIDByParentID(pk in number, cxitor_type_id number, pxitor_type_id number, config_fieldid in config_field.config_field_id%type) return varchar2;
   function getValStrStaticParent(pk number, c_xt_id  number,  p_xt_id  number, config_fieldname varchar2) return varchar2;
   function getValStrStaticParentID(pk number, c_xt_id  number,  p_xt_id  number) return varchar2;
   function getTaskDate(wpid number, xitorID number, subXitorID number, TemplateTaskID number, dateType number, SF number) return date;
   function getTaskDateSummary(wpid number, xitorID number, subXitorID number, TemplateTaskID number) return date;

   function getTNameDate(wpid number, xitorID number, subXitorID number, taskNameId number, dateType number, SF number) return date;
   function getTOrdDate(wpid number, xitorID number, subXitorID number, ordnumId number, dateType number, SF number) return date;
   function getTWbsDate(wpid number, xitorID number, subXitorID number, wbsId number, dateType number, SF number) return date;

   --Special functions for Report Wizard
   function getFluxxCF(pkey number, field varchar2) return date;
   function getFluxxCFParent(pkey number, c_xt_id  number,  p_xt_id  number, field varchar2) return date;
   function getFluxxXS(pkey number, xs_field_id  number, field number) return date;
   function getFluxxStaticXS(pkey number, xs_field_id  number,  cf_xt_id  number, field varchar2) return date;
   function getFluxxTask(wpid number, xitorID number, subXitorID number, TemplateTaskID number, dateType number, SF number) return date;

   --Returns Configured Date (for Task page)
   function getCFTaskDateByTaskID(taskid number, dateType number, SF number) return date;

   --Special functions for WorkFlow page
   function getWFValStrByIDYN (xitorPK number, subXitorPK number, p_cfxtid number, p_cfid number) return varchar2;
   function getWFValNumNLByID (xitorPK number, subXitorPK number, p_cfxtid number, p_cfid number) return varchar2;
   function getWFValDateByID (xitorPK number, subXitorPK number, p_cfxtid number, p_cfid number) return varchar2;

   /**
    * Returns collection of children Trackor IDs for the given Trackor ID
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    *
    * @return tableofnum
    */
    function get_trackor_ids_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type) return tableofnum;

   /**
    * Returns collection of parent Trackor IDs for the given Trackor ID
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_parent_tt_id   Parent trackor type id
    *
    * @return tableofnum
    */
    function get_trackor_ids_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type) return tableofnum;

   /**
    * Returns collection of numbers from the numeric CFs of children Trackors for the given parent Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    * @param p_child_cf_id    Config field id with data type number from child trackor type
    *
    * @return tableofnum
    */
    function get_val_num_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofnum;

   /**
    * Returns collection of varchar from the text CFs of children Trackors for the given parent Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    * @param p_child_cf_id    Config field id with data type number from child trackor type
    *
    * @return tableofchar
    */
    function get_val_char_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofchar;

   /**
    * Returns collection of clob from the memo, wiki CFs of children Trackors for the given parent Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    * @param p_child_cf_id    Config field id with data type number from child trackor type
    *
    * @return tableofclob
    */
    function get_val_clob_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofclob;

   /**
    * Returns collection of date from the date CFs of children Trackors for the given parent Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    * @param p_child_cf_id    Config field id with data type number from child trackor type
    *
    * @return tableofdate
    */
    function get_val_date_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofdate;

   /**
    * Returns collection of children Trackors Task dates for the given current Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_child_tt_id    Child trackor type id
    * @param p_template_workplan_id  Template Workplan ID
    * @param p_template_task_id  Template Task id
    * @param p_date_type_id  Task date type id
    * @param p_is_start  0 - Finish date, 1 - Start date 
    *
    * @return tableofdate
    */
    function get_task_date_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return tableofdate;

   /**
    * Returns collection of number from the number CFs of parent Trackors for the given child Trackor
    * Used just when cardinality type is many to many
    *
    * @param p_current_tid    Current trackor id that fired automation
    * @param p_current_tt_id  Current trackor type id that fired automation
    * @param p_parent_tt_id   Parent trackor type id
    * @param p_parent_cf_id   Config field id with data type number from parent trackor type
    *
    * @return tableofnum
    */
    function get_val_num_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofnum;

   /**
    * Returns collection of varchar from the text CFs of parent Trackors for the given child Trackor
    * Used just when cardinality type is many to many
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_parent_tt_id   Parent trackor type id
    * @param p_parent_cf_id   Config field id with data type text from parent trackor type
    *
    * @return tableofchar
    */
    function get_val_char_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofchar;

   /**
    * Returns collection of date from the date CFs of parent Trackors for the given child Trackor
    * Used just when cardinality type is many to many
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_parent_tt_id   Parent trackor type id
    * @param p_parent_cf_id   Config field id with data type date from parent trackor type
    *
    * @return tableofdate
    */
    function get_val_date_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofdate;

   /**
    * Returns collection of parent Trackor Task dates for the given current Trackor
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_parent_tt_id   Parent trackor type id
    * @param p_template_workplan_id  Template Workplan ID
    * @param p_template_task_id  Template Task id
    * @param p_date_type_id  Task date type id
    * @param p_is_start  0 - Finish date, 1 - Start date 
    *
    * @return tableofdate
    */
    function get_task_date_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return tableofdate;

   /**
    * Returns collection of clob from the memo, wiki CFs of parent Trackors for the given child Trackor
    * Used just when cardinality type is many to many
    *
    * @param p_current_tid    Current trackor id that fired automation
    * @param p_current_tt_id  Current trackor type id that fired automation
    * @param p_parent_tt_id   Parent trackor type id
    * @param p_parent_cf_id   Config field id with data type memo from parent trackor type
    *
    * @return tableofclob
    */
    function get_val_clob_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofclob;

   /**
    * Returns parent parent Trackor Id for the given child Trackor Id
    * Works correctly if there is one to many relation between parent and current(child) trackor type.
    *
    * @param p_current_tid    Current trackor id
    * @param p_current_tt_id  Current trackor type id
    * @param p_parent_tt_id   Parent trackor type id
    *
    * @return parent trackor id
    */
    function get_parent_tid(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type) return xitor.xitor_id%type;

    /**
     * Return task date value from active Workplan
     *
     * @param p_template_workplan_id  Template Workplan ID
     * @param p_trackor_id  Trackor id
     * @param p_template_task_id  Template Task id
     * @param p_date_type_id  Task date type id
     * @param p_is_start  0 - Finish date, 1 - Start date 
     */
    function get_task_date(
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_trackor_id       in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return date;

    /**
     * Return is Task NA or not
     *
     * @param p_template_workplan_id  Template Workplan ID
     * @param p_trackor_id  Trackor id
     * @param p_template_task_id  Template Task id
     *
     * @return 1 - Task is NA, 0 - not NA
     */
    function is_task_na(
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_trackor_id       in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type) return number;

    /**
     * Return file name of BLOB
     *
     * @param p_blob_data_id ID of BLOB in BLOB_DATA table
     *
     * @return file name of BLOB
     */
     function get_file_name_by_blob_id(p_blob_data_id in blob_data.blob_data_id%type) return blob_data.filename%type;

    /**
     * Returns collection of t_efile_blob with EFile blob data - blob_data_id, file_name, file_size as 
     *
     * @param p_pk Entity ID - Trackor ID, WorkPlan ID, WP Task ID or Workflow ID
     * @param p_cfid ID of config field
     * @return collection list_efile_blob
     */
     function get_efile_blob_data(p_pk in number, p_cfid in config_field.config_field_id%type) return list_efile_blob;

    /**
     * The function executes SQL of Linked Value and return result of first row and first column.
     * The SQL supports optional bind variable ":PK"
     *
     * @param p_entity_id ID of entity
     * @param p_sql <code>multiplier_sql_query</code> SQL for calculation Linked Value multiplier
     *
     * @return Linked Value multiplier or NULL if SQL is invalid or returns nothing
     */
    function exec_linked_value_sql(
        p_entity_id in number,
        p_sql in config_field_linked_value.multiplier_sql_query%type) return number;

    /**
     * The function returns sorted collection of linked values related to a config field for current value
     *
     * @param p_cfid <code>config_field_id</code> Config Field ID which we calculate linked values for
     * @param p_entity_id ID of entity
     *
     * @return Sorted collection of linked values
     */
    function get_linked_values_by_entity_id(
        p_cfid in config_field.config_field_id%type,
        p_entity_id in number,
        p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar;

    /**
     * The function returns sorted collection of linked values related to a config field by passed original value
     *
     * @param p_cfid <code>config_field_id</code> Config Field ID which we calculate linked values for
     * @param p_cf_value Original CF value used in calculation of linked values
     *
     * @return Sorted collection of linked values
     */
    function get_linked_values_by_cf_value(
         p_cfid in config_field.config_field_id%type,
         p_cf_value_number in config_value_number.value_number%type,
         p_entity_id in number,
         p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar;

    /**
     * The function returns collection of linked values holders
     *
     * @param p_cf_value_numbers <code>list_config_value_number</code> Collection of original values which need to calculate linked values for.
     *                           The collection item includes <code>config_field_id</code>, <code>key_value</code> and number value
     *
     * @return Collection linked values holders. The holder includes <code>config_field_id</code>, <code>key_value</code> and collection of linked values
     */
    function get_linked_values(
        p_cf_value_numbers in list_config_value_number,
        p_thousands_separator in users.thousands_separator%type default ' ') return list_linked_values_holder;

    /**
     * The function returns sorted collection of linked values related to a drill down config field for current value
     *
     * @param p_entity_id ID of entity
     * @param p_xs_cfid <code>config_field_id</code> Drill Down Trackor Selector field ID with help of we get access to drill down field
     * @param p_cfid <code>config_field_id</code> Config Field ID which we calculate linked values for
     *
     * @return Sorted collection of linked values
     */
    function get_linked_values_by_drill_down_entity_id(
        p_entity_id in number,
        p_xs_cfid in config_field.config_field_id%type,
        p_drill_down_cfid config_field.config_field_id%type,
        p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar;


    /**
     *  Checks if MultiSelector or Trackor MultiSelector field has provided ID as its selected value.
     */
    function is_value_in_mult (
        p_pk in config_value_mult.key_value%type,
        p_config_field_id in config_field.config_field_id%type,
        p_value in config_value_mult.value_number%type) return boolean;


    /**
     *  Returns values collection for MultiSelector or Trackor MultiSelector field.
     */
    function get_str_mult_by_id(
        p_pk in config_value_mult.key_value%type,
        p_config_field_id in config_field.config_field_id%type) return tableofchar;

    /**
     *  Returns values collection for a drill-down MultiSelector or Trackor MultiSelector field.
     *
     * @param p_xs_pk Trackor ID for Drill-Down Trackor Selector
     * @param p_xs_cfid Config field ID of Drill-Down Trackor Selector
     * @param p_cfid Config field ID for which this function returns a values collection
     * @return Collection of Trackor Keys as a TABLEOFCHAR
     */
    function get_str_mult_by_id_xs(
        p_xs_pk in config_value_mult.key_value%type,
        p_xs_cfid in config_field.config_field_id%type,
        p_cfid config_field.config_field_id%type) return tableofchar;

    /**
     * Returns Task Date value if task is found and is not NA. 
     *         If task is not found or p_wp_id is null or p_tid is null - returns NULL.
     *         If Task is NA then returns value using following rules:
     *         If p_export_na_task_mode = gc_export_na_task_mode_blank        - returns null
     *         If p_export_na_task_mode = gc_export_na_task_mode_task_date    - returns selected Task Date value
     *         If p_export_na_task_mode = gc_export_na_task_mode_static_date  - returns value from p_date_value_for_mode_static_date
     *         If p_export_na_task_mode = gc_export_na_task_mode_na           - returns constant date value declared in C_VALUE_FOR_NA
     *
     * @param p_wp_id Workplan ID
     * @param p_tid Parent or Child Trackor ID, dependents of the task level
     * @param p_template_task_id Template Task ID
     * @param p_dtid Date Type ID
     * @param p_is_finish_date 1 - Finish date, 0 - Start date
     * @param p_export_na_task_mode Export mode for NA tasks. 
     *                              One of the following constant values from pkg_wp_template:
     *                              gc_export_na_task_mode_blank, gc_export_na_task_mode_task_date,
     *                              gc_export_na_task_mode_static_date, gc_export_na_task_mode_na.
     * @param p_date_value_for_mode_static_date Date value returned if p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_static_date
     * @return Date value
     */
    function get_task_date(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date;

    /**
     * Returns get_task_date function value by p_task_name_id
     */
    function get_task_date_by_task_name_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_name_id in wp_tasks_name_ids.task_name_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date;

    /**
     * Returns get_task_date function value by p_task_wbs_id
     */
    function get_task_date_by_task_wbs_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_wbs_id in wp_tasks_wbs_ids.wbs_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date;

    /**
     * Returns get_task_date function value by p_task_ordnum_id
     */
    function get_task_date_by_task_ordnum_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_ordnum_id in wp_tasks_ordnum_ids.ordnum_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date;
end pkg_config_field_rpt;
/
