CREATE OR REPLACE PACKAGE BODY PKG_IMP_RUN_COLUMN 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

    --TODO we should create constants in right package (example: in PKG_CF) and remove from this package and remove from pkg_wiz_dropgrid package
    /*CONFIG FIELD DATA TYPE*/
    c_cf_dt_text constant config_field.data_type%type := 0;
    c_cf_dt_number constant config_field.data_type%type := 1;
    c_cf_dt_date constant config_field.data_type%type := 2;
    c_cf_dt_checkbox constant config_field.data_type%type := 3;
    c_cf_dt_drop_down constant config_field.data_type%type := 4;
    c_cf_dt_memo constant config_field.data_type%type := 5;
    c_cf_dt_selector constant config_field.data_type%type := 10;
    c_cf_dt_latitude constant config_field.data_type%type := 11;
    c_cf_dt_longitude constant config_field.data_type%type := 12;
    c_cf_dt_datetime constant config_field.data_type%type := 90;

    --TODO we should create constants in right package (example: in PKG_IMP_RUN or in PKG_IMP_UTILS)
    /*IMP DATA TYPE*/
    c_imp_dt_config_field constant imp_data_type.imp_data_type_id%type := 1;

    procedure set_message(
        p_imp_run_id imp_run.imp_run_id%type,
        p_message varchar2)
    as
    begin
        if is_enable_logging then
            util.setmessage('pkg_imp_run_by_columns', 'imp_run_id [' || p_imp_run_id || ']', p_message);
        end if;
    end set_message;

    procedure load_cf(
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_pk number,
        p_dtfmt varchar2,
        p_val in out nocopy clob,
        p_ln number default 1,
        p_is_idval number default 0)
    as
        pragma autonomous_transaction;
        v_val_str varchar2(100);
    begin
        pkg_lob_utils.replace_clob(p_val, '''''', '''');

        if pkg_dl_support.AllowNulls = 1 and p_val = 'NULL' then
            p_val := null;
        end if;

        if p_cf_data_type = c_cf_dt_date then
            pkg_dl_support.set_cf_data(p_cf_id, p_pk, to_date(p_val, p_dtfmt), p_ln, p_is_idval);
        elsif p_cf_data_type = c_cf_dt_datetime then
            pkg_dl_support.set_cf_data(p_cf_id, p_pk, to_date(p_val, p_dtfmt), p_ln, p_is_idval);
        elsif p_cf_data_type = c_cf_dt_checkbox then
            v_val_str := pkg_imp_utils.convert_boolean(p_val);
            pkg_dl_support.set_cf_data(p_cf_id, p_pk, v_val_str, p_ln, p_is_idval);
        else
            pkg_dl_support.set_cf_data(p_cf_id, p_pk, p_val, p_ln, p_is_idval);
        end if;

        commit;
    end load_cf;

    procedure load_cf_col_num(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_count_changes number := 0;
    begin
        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_num start :=' || v_count_changes || ' ' || p_col_name);

        for rec in (select pk.pk xitor_id, g.data, g.row_num
                    from imp_run_entity_pk pk
                         join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_col_id)
                         left outer join config_value_number v on (v.key_value = pk.pk and v.config_field_id = p_cf_id)
                    where pk.imp_run_id = p_imp_run_id
                          and pk.imp_entity_id = p_entity_id
                          and decode(g.data, null, '-111111', 'NULL', '-111111', g.data) <> to_char(nvl(v.value_number, '-111111'))
                    order by pk.row_num)
        loop
            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, null, rec.data, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_num finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_num;

    procedure load_cf_col_checkbox(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_count_changes number := 0;
    begin
        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_checkbox start :=' || v_count_changes || ' ' || p_col_name);

        for rec in (select pk.pk xitor_id, g.data, g.row_num
                    from imp_run_entity_pk pk
                         join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_col_id)
                         left outer join config_value_number v on (v.key_value = pk.pk and v.config_field_id = p_cf_id)
                    where pk.imp_run_id = p_imp_run_id
                          and pk.imp_entity_id = p_entity_id
                          and decode(data, null, '0', 'NULL', '0', 'Y', '1', 'YES', '1', 'ALL', '1', 'TRUE', '1', 'true', '1', data) <> to_char(nvl(v.value_number, '0'))
                    order by pk.row_num)
        loop
            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, null, rec.data, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_checkbox finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_checkbox;

    procedure load_cf_col_date(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type,
        p_date_format imp_spec.date_format%type,
        p_time_format imp_spec.time_format%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_count_changes number := 0;
        v_dtfmt varchar2(100);
    begin
        v_dtfmt := p_date_format || ' ' || p_time_format;

        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_date start :=' || v_count_changes || ' ' || p_col_name);

        for rec in (select pk.pk xitor_id, g.data, g.row_num
                    from imp_run_entity_pk pk
                         join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_col_id)
                         left outer join config_value_date v on (v.key_value = pk.pk and v.config_field_id = p_cf_id)
                    where pk.imp_run_id = p_imp_run_id
                          and pk.imp_entity_id = p_entity_id
                          and decode(data, null, to_date('1/1/1900', 'mm/dd/yyyy'), 'NULL', to_date('1/1/1900', 'mm/dd/yyyy'), to_date(data, v_dtfmt)) <> nvl(v.value_date, to_date('1/1/1900', 'mm/dd/yyyy'))
                    order by pk.row_num)
        loop
            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, v_dtfmt, rec.data, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_date finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_date;

    procedure load_cf_col_str(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_count_changes number := 0;
    begin
        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_str start :=' || v_count_changes || ' ' || p_col_name);

        for rec in (select pk.pk xitor_id, g.data, g.row_num
                    from imp_run_entity_pk pk
                         join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_col_id)
                         left outer join config_value_char v on (v.key_value = pk.pk and v.config_field_id = p_cf_id)
                    where pk.imp_run_id = p_imp_run_id
                          and pk.imp_entity_id = p_entity_id
                          and decode(data, null, '|||||-|||||', 'NULL', '|||||-|||||', data) <> nvl(v.value_char, '|||||-|||||')
                    order by pk.row_num)
        loop
            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, null, rec.data, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_str finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_str;

    procedure load_cf_col_memo(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_count_changes number := 0;
    begin
        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_memo start :=' || v_count_changes || ' ' || p_col_name);

        for rec in (select pk.pk xitor_id, nvl(g.clob_data, g.data) data, g.row_num
                      from imp_run_entity_pk pk
                      join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_col_id)
                      left outer join config_value_char v on (v.key_value = pk.pk and v.config_field_id = p_cf_id)
                     where pk.imp_run_id = p_imp_run_id
                       and pk.imp_entity_id = p_entity_id
                       and (   dbms_lob.compare(nvl(g.clob_data, g.data), nvl(v.value_clob, v.value_char)) <> 0 
                            or (g.data is not null and g.data <> 'NULL' and v.value_char is null) 
                            or ((g.data is null or g.data = 'NULL') and v.value_char is not null)
                           )
                    order by pk.row_num)
        loop
            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, null, rec.data, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_memo finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_memo;

    procedure load_cf_col_dropdown(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_cf_id config_field.config_field_id%type,
        p_cf_data_type config_field.data_type%type,
        p_col_name imp_column.name%type)
    as
        v_col_id imp_run_grid.col_num%type;
        v_err_msg imp_run_error.error_msg%type;
        v_count_changes number := 0;
        v_data varchar2(100);
        v_vtab_id config_field.attrib_v_table_id%type;
        v_ord_num attrib_v_table_value.order_num%type;
        v_vtab_val_id attrib_v_table_value.attrib_v_table_value_id%type;
    begin
        v_col_id := pkg_imp_utils.get_col_num(p_imp_run_id, p_col_name);
        if v_col_id is null then
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => 'Column [' || p_col_name || '] is missing', 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        set_message(p_imp_run_id, 'load_cf_col_dropdown start :=' || v_count_changes || ' ' || p_col_name);

        select attrib_v_table_id into v_vtab_id from config_field where config_field_id = p_cf_id;

        if (v_vtab_id is null) then
            v_err_msg := 'V Table is not set for Configured Field ID: [' || p_cf_id || ']' || chr(10) || 'Column: [' || p_col_name || '] will be skipped';
            pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                    p_msg => v_err_msg, 
                                    p_err_type_id => pkg_imp_run.c_et_data, 
                                    p_row_num => 0,
                                    p_col_name => p_col_name);
            return;
        end if;

        for rec in (select pk.pk xitor_id, tv.attrib_v_table_value_id, g.data, g.row_num
                    from imp_run_entity_pk pk
                    join (select row_num, regexp_replace(data, '(^[[:space:]]+)|([[:space:]]+$)', null) data 
                          from imp_run_grid where imp_run_id = p_imp_run_id and col_num = v_col_id) g 
                    on (g.row_num = pk.row_num)
                    left outer join attrib_v_table_value tv on (tv.value = g.data and tv.attrib_v_table_id = v_vtab_id )
                    left outer join config_value_number vn 
                    on (vn.value_number = tv.attrib_v_table_value_id and vn.config_field_id = p_cf_id and vn.key_value = pk.pk)
                    left outer join config_value_number vn2 on (vn2.config_field_id = p_cf_id and vn2.key_value = pk.pk)
                    where pk.imp_run_id = p_imp_run_id and pk.imp_entity_id = p_entity_id and vn.value_number is null
                    and not(g.data is null and vn2.value_number is null))
        loop
            if (rec.attrib_v_table_value_id is null and rec.data is not null) then
                -- add new v_ table value
                select nvl(max(order_num),0) + 1 into v_ord_num
                from attrib_v_table_value
                where attrib_v_table_id = v_vtab_id;

                begin
                    insert into attrib_v_table_value(attrib_v_table_id, value, order_num)
                    values (v_vtab_id, rec.data, v_ord_num)
                    returning attrib_v_table_value_id into v_vtab_val_id;
                exception 
                    when dup_val_on_index then
                        --Value already inserted on previous iteration
                        null;
                end;

                v_data := to_char(v_vtab_val_id);

            else
                v_data := to_char(rec.attrib_v_table_value_id);
            end if;

            begin
                load_cf(p_cf_id, p_cf_data_type, rec.xitor_id, null, v_data, 1, 1);
            exception
                when others then
                    pkg_imp_run.write_error(p_rid => p_imp_run_id, 
                                            p_msg => sqlerrm, 
                                            p_err_type_id => pkg_imp_run.c_et_data, 
                                            p_row_num => rec.row_num,
                                            p_entity_id => rec.xitor_id,
                                            p_col_name => p_col_name,
                                            p_bad_data_value => rec.data);
            end;
            v_count_changes := v_count_changes + 1;
        end loop;

        set_message(p_imp_run_id, 'load_cf_col_dropdown finish :=' || v_count_changes || ' ' || p_col_name);
    end load_cf_col_dropdown;


    function entity_prep_sql_replace_params(
        p_entity_id imp_entity.imp_entity_id%type,
        p_entity_sql imp_entity.sql_text%type,
        p_imp_run in pkg_imp_run.cur_imp_run%rowtype)
    return imp_entity.sql_text%type
    as
        v_varval varchar2(255);
        v_entity_sql imp_entity.sql_text%type;
    begin
        v_entity_sql := p_entity_sql;

        for param in (select imp_column_id, parameter_value, sql_parameter
                        from imp_entity_param
                       where imp_entity_id = p_entity_id) loop
            if param.imp_column_id is not null then
                v_varval := pkg_imp_run.col_num(p_imp_run.imp_run_id, param.imp_column_id);
                --TODO we should add new column in DB table imp_entity_param
                -- to use col_num instead of cell_value
                --v_col_num := col_num(v_imp_run.imp_run_id, param.imp_column_id);
                --cell_value(v_imp_run.imp_run_id, v_row_num, v_col_num, varval);
            else
                if param.parameter_value = 'USER_ID' then
                    v_varval := p_imp_run.user_id;
                elsif param.parameter_value = 'PROGRAM_ID' then
                    v_varval := p_imp_run.program_id;
                elsif param.parameter_value = 'DATE_FORMAT' then
                    v_varval := p_imp_run.date_format;
                elsif param.parameter_value = 'IMP_RUN_ID' then
                    v_varval := p_imp_run.imp_run_id;
                --TODO ROW_NUM not support because we execute one sql for all rows
                --elsif param.parameter_value = 'ROW_NUM' then
                --    varval := v_row_num;
                else
                    v_varval := param.parameter_value;
                end if;
            end if;
            v_entity_sql := pkg_imp_run.ireplace(v_entity_sql, ':' || param.sql_parameter, v_varval);
        end loop;

        return v_entity_sql;
    end entity_prep_sql_replace_params;

    function entity_prep_sql_attach_insert(
        p_entity_id imp_entity.imp_entity_id%type,
        p_entity_sql imp_entity.sql_text%type,
        p_imp_run in pkg_imp_run.cur_imp_run%rowtype)
    return imp_entity.sql_text%type
    as
        v_entity_sql imp_entity.sql_text%type;
    begin
        v_entity_sql := p_entity_sql;

        if instr(v_entity_sql, 'select xitor_id, row_num') is not null and instr(v_entity_sql, 'select xitor_id, row_num') = 1 then
            v_entity_sql := substr(v_entity_sql, length('select xitor_id, row_num') + 1);
            v_entity_sql := 'select xitor_id, row_num' || ', ' || p_entity_id || ', ' || p_imp_run.imp_run_id || ', ' || p_imp_run.program_id || ', ' || 'decode(xitor_id, null, 1, 0)' || v_entity_sql;
        end if;
        v_entity_sql := 'insert into imp_run_entity_pk(pk, row_num, imp_entity_id, imp_run_id, program_id, is_inserted)' || chr(13) || chr(10) || v_entity_sql;

        return v_entity_sql;
    end entity_prep_sql_attach_insert;

    function entity_prep_sql_remove_semicol(
        p_entity_sql imp_entity.sql_text%type)
    return imp_entity.sql_text%type
    as
        v_entity_sql imp_entity.sql_text%type;
    begin
        v_entity_sql := p_entity_sql;

        v_entity_sql := substr(v_entity_sql, 1, length(v_entity_sql) - 1);

        return v_entity_sql;
    end entity_prep_sql_remove_semicol;

    function entity_prep_sql(
        p_entity_id imp_entity.imp_entity_id%type,
        p_entity_sql imp_entity.sql_text%type,
        p_imp_run pkg_imp_run.cur_imp_run%rowtype)
    return imp_entity.sql_text%type
    as
        v_entity_sql imp_entity.sql_text%type;
    begin
        v_entity_sql := p_entity_sql;
        v_entity_sql := entity_prep_sql_replace_params(p_entity_id, v_entity_sql, p_imp_run);
        v_entity_sql := entity_prep_sql_attach_insert(p_entity_id, v_entity_sql, p_imp_run);
        v_entity_sql := entity_prep_sql_remove_semicol(v_entity_sql);

        util.SetMessage('', 'v_entity_sql=', v_entity_sql);



        --if is_enable_logging then
        --    util.setmessage('pkg_ext_imp_imn_mm_bom', 'p_rid ' || p_imp_run_id, 'Start generate_xitor_keys');
        --    util.setmessage('pkg_imp_run_by_columns', 'p_rid ' || p_rid, 'load_cf_col_num start :=' || l_i || ' ' || p_col_name);
        --end if;

        return v_entity_sql;
    end entity_prep_sql;

    procedure entity_fill_pks(
        p_rid in imp_run_error.imp_run_id%type,
        p_sql in out nocopy clob,
        p_err_type_id in imp_run_error.imp_error_type_id%type)
    as
    pragma autonomous_transaction;
        --v_value number;
        v_errmsg imp_run_error.error_msg%type;
        --ssql2 clob;
    begin
        --ssql2:= replace(p_sql, chr(13), '');
        --ssql2:= replace(ssql2, chr(10), ' ');

        execute immediate p_sql;
        commit;
        --return v_value;
    exception
        when others then
            v_errmsg := dbms_utility.format_error_stack;
            pkg_imp_run.write_error(p_rid, v_errmsg, p_err_type_id, 0, p_sql);
    end entity_fill_pks;

    procedure entity_create_trackors(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type)
    as
    pragma autonomous_transaction;
        v_xitor_id number;
        v_xitor_type_id number;
        v_errmsg imp_run_error.error_msg%type;
        v_is_use_xitor_key_from_import boolean := false;
        v_xitor_key_col_num number;
    begin
        select xt.xitor_type_id into v_xitor_type_id
        from imp_entity e
             join xitor_type xt on (e.xitor_type_id = xt.xitor_type_id)
        where e.imp_entity_id = p_entity_id;

        for reqfld in (select e.field_name,i.*
                       from imp_entity_req_field i,
                            xitor_req_field e
                       where i.xitor_req_field_id = e.xitor_req_field_id
                             and i.imp_entity_id = p_entity_id)
        loop
            if reqfld.field_name = 'TRACKOR_KEY' and reqfld.imp_column_id is not null then
                v_is_use_xitor_key_from_import := true;
                v_xitor_key_col_num := pkg_imp_run.col_num(p_imp_run_id, reqfld.imp_column_id);
            end if;
        end loop;

        if v_is_use_xitor_key_from_import then
            for rec in (select pk.imp_run_entity_pk_id, pk.row_num, g.data xitor_key
                        from imp_run_entity_pk pk
                             join imp_run_grid g on (g.imp_run_id = pk.imp_run_id and g.row_num = pk.row_num and g.col_num = v_xitor_key_col_num)
                        where pk.imp_run_id = p_imp_run_id
                              and pk.imp_entity_id = p_entity_id
                              and pk.is_inserted = 1
                              and pk.pk is null)
            loop
                begin
                    v_xitor_id := pkg_xitor.new_xitor(
                        p_xitor_type_id     => v_xitor_type_id,
                        p_xitor_key         => rec.xitor_key,
                        p_is_template       => null,
                        p_template_xitor_id => null,
                        p_user_id           => null,
                        p_program_id        => pkg_sec.get_pid,
                        p_xitor_class_id    => null);
                    update imp_run_entity_pk set pk = v_xitor_id where imp_run_entity_pk_id = rec.imp_run_entity_pk_id;
                    commit;
                exception
                    when others then
                        v_errmsg := dbms_utility.format_error_stack;
                        pkg_imp_run.write_error(p_imp_run_id, v_errmsg, pkg_imp_run.c_et_pk, rec.row_num);
                        rollback;
                end;
            end loop;
        else
            for rec in (select pk.imp_run_entity_pk_id, pk.row_num, null xitor_key
                        from imp_run_entity_pk pk
                        where pk.imp_run_id = p_imp_run_id
                              and pk.imp_entity_id = p_entity_id
                              and pk.is_inserted = 1
                              and pk.pk is null)
            loop
                begin
                    v_xitor_id := pkg_xitor.new_xitor(
                        p_xitor_type_id     => v_xitor_type_id,
                        p_xitor_key         => rec.xitor_key,
                        p_is_template       => null,
                        p_template_xitor_id => null,
                        p_user_id           => null,
                        p_program_id        => pkg_sec.get_pid,
                        p_xitor_class_id    => null);
                    update imp_run_entity_pk set pk = v_xitor_id where imp_run_entity_pk_id = rec.imp_run_entity_pk_id;
                    commit;
                exception
                    when others then
                        v_errmsg := dbms_utility.format_error_stack;
                        pkg_imp_run.write_error(p_imp_run_id, v_errmsg, pkg_imp_run.c_et_pk, rec.row_num);
                        rollback;
                end;
            end loop;
        end if;
    exception when others then
        rollback;
        raise;
    end entity_create_trackors;

    procedure entity_import_mappings(
        p_imp_run_id imp_run.imp_run_id%type,
        p_entity_id imp_entity.imp_entity_id%type,
        p_date_format imp_spec.date_format%type,
        p_time_format imp_spec.time_format%type)
    as
        v_cf_id number;
        v_cf_data_type number;
    begin
        for cur in (select dt.imp_data_type_id, dt.name, dm.imp_data_map_id, dm.data_map_name, dm.imp_column_id, c.name col_name
                    from imp_data_map dm
                         join imp_data_type dt on (dt.imp_data_type_id = dm.imp_data_type_id)
                         left join imp_column c on (c.imp_column_id = dm.imp_column_id)
                    where dm.imp_entity_id = p_entity_id)
        loop
            if cur.imp_data_type_id = c_imp_dt_config_field then
                if cur.imp_column_id is not null then
                    begin
                        select to_number(value) into v_cf_id
                        from imp_data_type_param_value
                        where imp_data_map_id = cur.imp_data_map_id
                              and imp_data_type_param_id = 1;
                    exception
                        when others then
                            pkg_imp_run.write_error(p_imp_run_id, 'CF not set. Mapping ['|| cur.data_map_name ||']. Mapping will be skipped', pkg_imp_run.c_et_data, 0);
                            continue;
                    end;

                    begin
                        select data_type into v_cf_data_type
                        from config_field
                        where config_field_id = v_cf_id;
                    exception
                        when others then
                            pkg_imp_run.write_error(p_imp_run_id, 'CF [' || v_cf_id || '] not exists. Mapping ['|| cur.data_map_name ||']. Mapping will be skipped', pkg_imp_run.c_et_data, 0);
                            continue;
                    end;

                    if v_cf_data_type in (c_cf_dt_text) then
                        load_cf_col_str(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name);
                    elsif v_cf_data_type in (c_cf_dt_memo) then
                        load_cf_col_memo(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name);
                    elsif v_cf_data_type in (c_cf_dt_date, c_cf_dt_datetime) then
                        load_cf_col_date(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name, p_date_format, p_time_format);
                    elsif v_cf_data_type in (c_cf_dt_number, c_cf_dt_latitude, c_cf_dt_longitude) then
                        load_cf_col_num(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name);
                    elsif v_cf_data_type in (c_cf_dt_drop_down, c_cf_dt_selector) then
                        load_cf_col_dropdown(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name);
                    elsif v_cf_data_type in (c_cf_dt_checkbox) then
                        load_cf_col_checkbox(p_imp_run_id, p_entity_id, v_cf_id, v_cf_data_type, cur.col_name);
                    else
                        pkg_imp_run.write_error(p_imp_run_id, 'Not supported CF data type [' || v_cf_data_type || ']. Mapping ['|| cur.data_map_name ||']. Mapping will be skipped', pkg_imp_run.c_et_data, 0);
                    end if;
                else
                    pkg_imp_run.write_error(p_imp_run_id, 'Mapping Data Type ['|| cur.name ||'] supports only configuration with column. Mapping ['|| cur.data_map_name ||']. Mapping will be skipped', pkg_imp_run.c_et_data, 0);
                end if;
            else
                pkg_imp_run.write_error(p_imp_run_id, 'Not supported Mapping Data Type [' || cur.name || ']. Mapping ['|| cur.data_map_name ||']. Mapping will be skipped', pkg_imp_run.c_et_data, 0);
            end if;
        end loop;
    end entity_import_mappings;

    procedure import(
        p_imp_run_id imp_run.imp_run_id%type,
        p_is_enable_logging boolean default false)
    as
        v_process_id process.process_id%type;
        v_imp_spec_id imp_spec.imp_spec_id%type;
        v_date_format imp_spec.date_format%type;
        v_time_format imp_spec.time_format%type;

        v_imp_run pkg_imp_run.cur_imp_run%rowtype;
        v_entity_sql imp_entity.sql_text%type;
    begin
        is_enable_logging := p_is_enable_logging;

        select ir.process_id, ir.imp_spec_id, iss.date_format, iss.time_format into v_process_id, v_imp_spec_id, v_date_format, v_time_format
        from imp_run ir
             join imp_spec iss on (iss.imp_spec_id = ir.imp_spec_id)
        where ir.imp_run_id = p_imp_run_id;

        open pkg_imp_run.cur_imp_run(p_imp_run_id);
        fetch pkg_imp_run.cur_imp_run into v_imp_run;
        close pkg_imp_run.cur_imp_run;

        --we should complete all actions for entity
        --and only after then go to next entity
        --because one entity can depend on other entity
        for cur in (select imp_entity_id, sql_text
                    from imp_entity
                    where imp_spec_id = v_imp_spec_id
                    order by order_number)
        loop
            v_entity_sql := entity_prep_sql(cur.imp_entity_id, cur.sql_text, v_imp_run);
            entity_fill_pks(p_imp_run_id, v_entity_sql, pkg_imp_run.c_et_new_xitor);
            entity_create_trackors(p_imp_run_id, cur.imp_entity_id);
            entity_import_mappings(p_imp_run_id, cur.imp_entity_id, v_date_format, v_time_format);
        end loop;
    end import;

end pkg_imp_run_column;
/