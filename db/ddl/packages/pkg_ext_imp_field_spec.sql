CREATE OR REPLACE PACKAGE PKG_EXT_IMP_FIELD 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
    procedure configuredfieldload(rid imp_run.imp_run_id%type);
    procedure configuredfields(p_rid in imp_run.imp_run_id%type);

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
end pkg_ext_imp_field;
/