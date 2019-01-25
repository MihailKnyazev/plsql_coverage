CREATE OR REPLACE PACKAGE PKG_CONFIG_FIELD_RPT 
/**
 * This package is used to help write configured field reports. It mostly relates
 * to finding field values so that they can be used to fill reports.
 */
    authid current_user
is
   TextCellsDisplayLimit number := 0;

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
    *
    * @param pk the <code>key_value</code> number
    * @param config_fieldid the <code>config_field_id</code> number
    * @return the <code>value_number</code> ID number
    */
   function getValNumNLByID(pk number, config_fieldid number) return number;

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

   function isFiledInUse(config_fieldid number) return number;

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
   function getTaskTypeDate(wpid number, xitorID number, subXitorID number, taskTypeId number, payLoadText varchar2, payLoadNum number, dateType number, SF number) return date;
   function getTaskDateSummary(wpid number, xitorID number, subXitorID number, TemplateTaskID number) return date;
   function getTaskTypeDateSummary(wpid number, xitorID number, subXitorID number, taskTypeId number, payLoadText varchar2, payLoadNum number) return date;

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

end pkg_config_field_rpt;
/