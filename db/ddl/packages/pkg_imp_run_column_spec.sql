CREATE OR REPLACE PACKAGE PKG_IMP_RUN_COLUMN 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

    --TODO implement loop with increment of 1000 in function entity_fill_pks ?
    --TODO implement loop with increment of 1000 in functions load_cf_col_... ?
    --TODO implement relation in function entity_import_mappings ?

    /**
     * This variable should be set to true to enable logging
     */
    is_enable_logging boolean := false;

    procedure import(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_is_enable_logging boolean default false);

    procedure load_cf_col_num(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type);

    procedure load_cf_col_checkbox(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type);

    procedure load_cf_col_date(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type,
        p_date_format imp_spec.date_format%type,
        p_time_format imp_spec.time_format%type);

    procedure load_cf_col_str(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type);

    procedure load_cf_col_memo(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type);

    procedure load_cf_col_dropdown(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type);

end pkg_imp_run_column;

/

