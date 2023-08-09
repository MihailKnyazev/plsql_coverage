CREATE OR REPLACE PACKAGE BODY PKG_EXT_IMP_FIELD 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
    cursor cur_cf_load_line(p_rid in imp_run.imp_run_id%type)
    is
    select p_rid as imp_run_id,
           row_num,
           component,
           upper(field) as field,
           field_id,
           label,
           description,
           comments,
           datatype,
           maxlength,
           fieldwidth,
           lines,
           readonly,
           vtable,
           is_lockable,
           is_mandatory,
           is_two_cols_span,
           log_blob_changes,
           obj_xitor_type,
           sql_query,
           use_in_my_things_filter,
           del,
           show_expanded,
           is_maskable,
           tab_trackor_type,
           tab_name,
           tab_label
      from (select row_num, data, pkg_imp_utils.get_col_name(p_rid, col_num) as col_name
              from imp_run_grid
             where imp_run_id = p_rid
               and row_num > (select start_row
                                from imp_run
                               where imp_run_id = p_rid))
     pivot (min(data) for col_name in ('XITOR_TYPE' as component,
                                       'CONFIG_FIELD_NAME' as field,
                                       'CONFIG_FIELD_ID' as field_id,
                                       'APP_LABEL_TEXT' as label,
                                       'DESCRIPTION' as description,
                                       'COMMENTS' as comments,
                                       'DATA_TYPE' as datatype,
                                       'FIELD_SIZE' as maxlength,
                                       'FIELD_WIDTH' as fieldwidth,
                                       'LINES_QTY' as lines,
                                       'IS_READ_ONLY' as readonly,
                                       'TABLE_NAME' as vtable,
                                       'IS_LOCKABLE' as is_lockable,
                                       'IS_MANDATORY' as is_mandatory,
                                       'IS_TWO_COLS_SPAN' as is_two_cols_span,
                                       'LOG_BLOB_CHANGES' as log_blob_changes,
                                       'OBJ_XITOR_TYPE' as obj_xitor_type,
                                       'SQL_QUERY' as sql_query,
                                       'USE_IN_MY_THINGS_FILTER' as use_in_my_things_filter,
                                       'DELETE' as del,
                                       'SHOW_EXPANDED' as show_expanded,
                                       'IS_MASKABLE' as is_maskable,
                                       'TAB_TRACKOR_TYPE' as tab_trackor_type,
                                       'TAB_NAME' as tab_name,
                                       'TAB_LABEL' as tab_label));

    cursor cur_cf_line(p_rid in imp_run.imp_run_id%type)
    is
    select p_rid as imp_run_id,
           row_num,
           component,
           upper(field) as field,
           field_id,
           label,
           description,
           comments,
           datatype,
           maxlength,
           fieldwidth,
           lines,
           readonly,
           vtable,
           is_lockable,
           is_mandatory,
           is_two_cols_span,
           log_blob_changes,
           obj_xitor_type,
           sql_query,
           use_in_my_things_filter,
           del,
           show_expanded,
           is_maskable,
           tab_trackor_type,
           tab_name,
           tab_label
      from (select row_num, data, pkg_imp_utils.get_col_name(p_rid, col_num) as col_name
              from imp_run_grid
             where imp_run_id = p_rid
               and row_num > (select start_row
                                from imp_run
                               where imp_run_id = p_rid))
     pivot (min(data) for col_name in ('TRACKOR_TYPE' as component,
                                       'CONFIG_FIELD_NAME' as field,
                                       'CONFIG_FIELD_ID' as field_id,
                                       'LABEL_TEXT' as label,
                                       'DESCRIPTION' as description,
                                       'COMMENTS' as comments,
                                       'DATA_TYPE' as datatype,
                                       'FIELD_SIZE' as maxlength,
                                       'FIELD_WIDTH' as fieldwidth,
                                       'LINES_QTY' as lines,
                                       'IS_READ_ONLY' as readonly,
                                       'TABLE_NAME' as vtable,
                                       'IS_LOCKABLE' as is_lockable,
                                       'IS_MANDATORY' as is_mandatory,
                                       'IS_TWO_COLS_SPAN' as is_two_cols_span,
                                       'LOG_BLOB_CHANGES' as log_blob_changes,
                                       'OBJ_XITOR_TYPE' as obj_xitor_type,
                                       'SQL_QUERY' as sql_query,
                                       'USE_IN_MY_THINGS_FILTER' as use_in_my_things_filter,
                                       'DELETE' as del,
                                       'SHOW_EXPANDED' as show_expanded,
                                       'IS_MASKABLE' as is_maskable,
                                       'TAB_TRACKOR_TYPE' as tab_trackor_type,
                                       'TAB_NAME' as tab_name,
                                       'TAB_LABEL' as tab_label));

    /**
      * Insert, update or delete configured field
      */
    function insert_cfield(
        p_line in cur_cf_load_line%rowtype, 
        rid in imp_run.imp_run_id%type)
        return config_field.config_field_id%type;

    /**
      * Insert or update configured group
      */
    procedure insert_config_group (
        p_line in cur_cf_line%rowtype,
        rid in imp_run.imp_run_id%type,
        p_cfid in config_field.config_field_id%type);

    /**
     * This procedure selects the appropriate row (either the first or second) that contains the headers for the fields.
     */
    procedure adjust_header_row(p_rid in imp_run.imp_run_id%type);

    /**
     * This function searches for trackor_type_id by the passed name or label
     */
    function get_ttid_by_label_or_name(p_label_or_name in varchar2) return xitor_type.xitor_type_id%type;


    /* Deprecated */
    procedure ConfiguredFieldLoad(rid imp_run.imp_run_id%type)
    as
        errmsg clob;
        rowcount number := 0;
        v_cfid config_field.config_field_id%type;
    begin
        adjust_header_row(rid);

        for line in cur_cf_load_line(rid)
        loop
            begin
                v_cfid := insert_cfield(line, rid);
                rowcount := rowcount + 1;
                pkg_imp_run.set_rows(rid, rowcount);
            exception
                when others then
                    errmsg := 'XITOR_TYPE: "' || line.component || '"' || chr(10) ||
                        'CONFIG_FIELD_NAME: "' || line.field || '"' || chr(10) ||
                        'CONFIG_FIELD_ID: "' || line.field_id || '"' || chr(10) ||
                        'APP_LABEL_TEXT: "' || line.label || '"' || chr(10) ||
                        'DESCRIPTION: "' || line.description || '"' || chr(10) ||
                        'DATA_TYPE: "' || line.datatype || '"' || chr(10) ||
                        'FIELD_SIZE: "' || line.maxlength || '"' || chr(10) ||
                        'FIELD_WIDTH: "' || line.fieldwidth || '"' || chr(10) ||
                        'LINES_QTY: "' || line.lines || '"' || chr(10) ||
                        'IS_READ_ONLY: "' || line.readonly || '"' || chr(10) ||
                        'TABLE_NAME: "' || line.vtable || '"' || chr(10) ||
                        'IS_LOCKABLE: "' || line.is_lockable || '"' || chr(10) ||
                        'IS_MANDATORY: "' || line.is_mandatory || '"' || chr(10) ||
                        'IS_TWO_COLS_SPAN: "' || line.is_two_cols_span || '"' || chr(10) ||
                        'LOG_BLOB_CHANGES: "' || line.log_blob_changes || '"' || chr(10) ||
                        'OBJ_XITOR_TYPE: "' || line.obj_xitor_type || '"' || chr(10) ||
                        'SQL_QUERY: "' || line.sql_query || '"' || chr(10) ||
                        'USE_IN_MY_THINGS_FILTER: "' || line.use_in_my_things_filter || '"' || chr(10) ||
                        'IS_MASKABLE: "' || line.is_maskable || '"' || chr(10) ||
                        'DELETE: "' || line.del || '"' || chr(10) ||
                        sqlerrm || chr(10) || dbms_utility.format_error_backtrace;
                    pkg_imp_run.write_error(rid, errmsg, pkg_imp_run.c_et_data, line.row_num);
            end;
        end loop;
    end ConfiguredFieldLoad;

    procedure ConfiguredFields(p_rid in imp_run.imp_run_id%type)
    is
        v_err_msg clob;
        v_row_count number := 0;
        v_cfid config_field.config_field_id%type;
    begin
        adjust_header_row(p_rid);

        for line in cur_cf_line(p_rid)
        loop
            begin
                v_cfid := insert_cfield(line, p_rid);
                v_row_count := v_row_count + 1;
                pkg_imp_run.set_rows(p_rid, v_row_count);
            exception when others then
                v_err_msg := 'TRACKOR_TYPE: "' || line.component || '"' || chr(10) ||
                    'CONFIG_FIELD_NAME: "' || line.field || '"' || chr(10) ||
                    'CONFIG_FIELD_ID: "' || line.field_id || '"' || chr(10) ||
                    'LABEL_TEXT: "' || line.label || '"' || chr(10) ||
                    'DESCRIPTION: "' || line.description || '"' || chr(10) ||
                    'DATA_TYPE: "' || line.datatype || '"' || chr(10) ||
                    'FIELD_SIZE: "' || line.maxlength || '"' || chr(10) ||
                    'FIELD_WIDTH: "' || line.fieldwidth || '"' || chr(10) ||
                    'LINES_QTY: "' || line.lines || '"' || chr(10) ||
                    'IS_READ_ONLY: "' || line.readonly || '"' || chr(10) ||
                    'TABLE_NAME: "' || line.vtable || '"' || chr(10) ||
                    'IS_LOCKABLE: "' || line.is_lockable || '"' || chr(10) ||
                    'IS_MANDATORY: "' || line.is_mandatory || '"' || chr(10) ||
                    'IS_TWO_COLS_SPAN: "' || line.is_two_cols_span || '"' || chr(10) ||
                    'LOG_BLOB_CHANGES: "' || line.log_blob_changes || '"' || chr(10) ||
                    'OBJ_XITOR_TYPE: "' || line.obj_xitor_type || '"' || chr(10) ||
                    'SQL_QUERY: "' || line.sql_query || '"' || chr(10) ||
                    'USE_IN_MY_THINGS_FILTER: "' || line.use_in_my_things_filter || '"' || chr(10) ||
                    'DELETE: "' || line.del || '"' || chr(10) ||
                    'SHOW_EXPANDED: "' || line.show_expanded || '"' ||chr(10) ||
                    'IS_MASKABLE: "' || line.is_maskable || '"' ||chr(10) ||
                    sqlerrm || chr(10) || dbms_utility.format_error_backtrace;
                pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            end;
        end loop;
    end ConfiguredFields;

    procedure ConfiguredFieldsWithTabs(p_rid in imp_run.imp_run_id%type)
    is
        v_err_msg clob;
        v_row_count number := 0;
        v_cfid config_field.config_field_id%type;
    begin
        adjust_header_row(p_rid);

        for line in cur_cf_line(p_rid)
        loop
            begin
                v_cfid := insert_cfield(line, p_rid);
                insert_config_group(line, p_rid, v_cfid);

                v_row_count := v_row_count + 1;
                pkg_imp_run.set_rows(p_rid, v_row_count);
            exception when others then
                v_err_msg := 'TRACKOR_TYPE: "' || line.component || '"' || chr(10) ||
                    'CONFIG_FIELD_NAME: "' || line.field || '"' || chr(10) ||
                    'CONFIG_FIELD_ID: "' || line.field_id || '"' || chr(10) ||
                    'LABEL_TEXT: "' || line.label || '"' || chr(10) ||
                    'DESCRIPTION: "' || line.description || '"' || chr(10) ||
                    'DATA_TYPE: "' || line.datatype || '"' || chr(10) ||
                    'FIELD_SIZE: "' || line.maxlength || '"' || chr(10) ||
                    'FIELD_WIDTH: "' || line.fieldwidth || '"' || chr(10) ||
                    'LINES_QTY: "' || line.lines || '"' || chr(10) ||
                    'IS_READ_ONLY: "' || line.readonly || '"' || chr(10) ||
                    'TABLE_NAME: "' || line.vtable || '"' || chr(10) ||
                    'IS_LOCKABLE: "' || line.is_lockable || '"' || chr(10) ||
                    'IS_MANDATORY: "' || line.is_mandatory || '"' || chr(10) ||
                    'IS_TWO_COLS_SPAN: "' || line.is_two_cols_span || '"' || chr(10) ||
                    'LOG_BLOB_CHANGES: "' || line.log_blob_changes || '"' || chr(10) ||
                    'OBJ_XITOR_TYPE: "' || line.obj_xitor_type || '"' || chr(10) ||
                    'SQL_QUERY: "' || line.sql_query || '"' || chr(10) ||
                    'USE_IN_MY_THINGS_FILTER: "' || line.use_in_my_things_filter || '"' || chr(10) ||
                    'DELETE: "' || line.del || '"' || chr(10) ||
                    'SHOW_EXPANDED: "' || line.show_expanded || '"' ||chr(10) ||
                    'IS_MASKABLE: "' || line.is_maskable || '"' ||chr(10) ||
                    'TAB_TRACKOR_TYPE: "' || line.tab_trackor_type || '"' || chr(10) ||
                    'TAB_NAME: "' || line.tab_name || '"' ||chr(10) ||
                    'TAB_LABEL: "' || line.tab_label || '"' ||chr(10) ||
                    sqlerrm || chr(10) || dbms_utility.format_error_backtrace;
                pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            end;
        end loop;
    end ConfiguredFieldsWithTabs;

    function insert_cfield(
        p_line in cur_cf_load_line%rowtype,
        rid in imp_run.imp_run_id%type)
        return config_field.config_field_id%type
    as
    pragma autonomous_transaction;
        v_xitor_type_id xitor_type.xitor_type_id%type;
        v_cf_id config_field.config_field_id%type;
        v_cfexists boolean;
        v_del boolean;
        v_cf config_field%rowtype;
        v_obj_xitor_type_id xitor_type.xitor_type_id%type;

        v_cf_name config_field.config_field_name%type;
        v_label_name varchar2(1000);
        v_pref_lbl label_program.label_program_text%type;
        v_field_size config_field.field_size%type;
        v_lines_qty config_field.lines_qty%type;
        v_description label_program.label_program_text%type;
        v_comments config_field.comments%type;
        v_app_label_id config_field.app_label_id%type;
        v_table_id config_field.attrib_v_table_id%type;
        v_data_type config_field.data_type%type;
        v_field_width config_field.field_width%type;
        v_is_read_only config_field.is_read_only%type;
        v_log_blob_changes config_field.log_blob_changes%type;
        v_is_mandatory config_field.is_mandatory%type;
        v_is_two_cols_span config_field.is_two_cols_span%type;
        v_sql_query config_field.sql_query%type;
        v_is_lockable config_field.is_lockable%type;
        v_use_in_my_things_filter config_field.use_in_my_things_filter%type;
        v_show_expanded config_field.show_expanded%type;
        v_pid program.program_id%type;
        v_description_label_id config_field.description_label_id%type;
        v_is_maskable config_field.is_maskable%type;
    begin
        select program_id into v_pid from imp_run where imp_run_id = rid;

        --Find xitor_type_id
        select f.xitor_type_id, pkg_label.get_label_system_program(f.prefix_label_id, pkg_sec.get_lang())
          into v_xitor_type_id, v_pref_lbl
          from xitor_type f, vw_label l
         where l.label_id = f.applet_label_id
           and l.app_lang_id = pkg_sec.get_lang
           and f.program_id = v_pid
           and (   upper(f.xitor_type) = upper(p_line.component)
                or upper(l.label_text) = upper(p_line.component));

        --Determine if Configured Field already exists, do nothing else if it does.
        begin
            select * into v_cf
              from config_field
             where config_field_id = p_line.field_id or (xitor_type_id = v_xitor_type_id and config_field_name = p_line.field)
               and (is_static = 0 or config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id));

            v_cf_id := v_cf.config_field_id;
            v_cfexists := true;
        exception
            when no_data_found then
                v_cf_id := null;
                v_cfexists := false;
        end;

        if v_cfexists and p_line.datatype is null and v_cf.is_static = 0 then
            v_data_type := v_cf.data_type;
        else
            begin
                select config_field_data_type_id into v_data_type
                  from config_field_data_type
                 where upper(type_name) = upper(p_line.datatype);
            exception
                when no_data_found then
                    raise_application_error(-20000, pkg_label.format(17661, pkg_label.list_label_params('data_type' => p_line.datatype)));
            end;

            if v_cfexists and v_data_type <> v_cf.data_type and v_cf.is_static = 1 then
                raise_application_error(-20000, pkg_label.format(17777, pkg_label.list_label_params('field_name' => p_line.field)));
            end if;

            if v_cfexists and v_data_type <> v_cf.data_type and pkg_config_field_rpt.isfieldinuse(v_cf_id) > 0 then
                 raise_application_error(-20000, pkg_label.format(17796, pkg_label.list_label_params('field_name' => p_line.field)));
            end if;
        end if;

        -- Convert FieldWidth into a number
        if p_line.fieldwidth is null then
            if v_cf.field_width is null then
                v_field_width := 180;
            else
                v_field_width := v_cf.field_width;
            end if;
        else
            v_field_width := to_number(p_line.fieldwidth);
        end if;

        -- Convert MaxLength into Number
        if p_line.maxlength is null then
            if v_cf.field_size is null then
                v_field_size := 50;
            else
                v_field_size := v_cf.field_size;
            end if;
        else
            v_field_size := to_number(p_line.maxlength);
        end if;

        --Convert Number of Lines into a Number
        if p_line.lines is null then
            if v_cf.lines_qty is not null then
                v_lines_qty := v_cf.lines_qty;
            elsif v_data_type in (pkg_cf.c_memo, pkg_cf.c_wiki) then
                v_lines_qty := 5;
            else
                v_lines_qty := null;
            end if;
        else
            v_lines_qty := to_number(p_line.lines);
        end if;

        -- Set stuff for VTable
        if p_line.vtable is not null then
            begin
                select attrib_v_table_id into v_table_id from attrib_v_table
                 where attrib_v_table_name = p_line.vtable and program_id = v_pid;
            exception
                when no_data_found then
                    insert into attrib_v_table (attrib_v_table_name, attrib_v_table_desc, program_id)
                        values (p_line.vtable, p_line.vtable, v_pid)
                    returning attrib_v_table_id into v_table_id;
            end;
        else
            v_table_id := v_cf.attrib_v_table_id;
        end if;

        if upper(trim(p_line.del)) in ('YES', 'Y', 'X', '1', 'DEL', 'DELETE') then
            v_del := true;
        else
            v_del := false;
        end if;

        v_description := p_line.description;
        v_comments := p_line.comments;
        v_is_read_only := pkg_imp_utils.convert_boolean(p_line.readonly);
        v_log_blob_changes := pkg_imp_utils.convert_boolean(p_line.log_blob_changes);
        v_is_mandatory := pkg_imp_utils.convert_boolean(p_line.is_mandatory);
        v_is_two_cols_span := pkg_imp_utils.convert_boolean(p_line.is_two_cols_span);
        v_sql_query := p_line.sql_query;
        v_is_lockable := pkg_imp_utils.convert_boolean(p_line.is_lockable);
        v_use_in_my_things_filter := pkg_imp_utils.convert_boolean(p_line.use_in_my_things_filter);
        v_show_expanded := pkg_imp_utils.convert_boolean(p_line.show_expanded);
        v_cf_name := p_line.field;
        v_label_name := p_line.label;
        v_is_maskable := pkg_imp_utils.convert_boolean(p_line.is_maskable);

        if v_cfexists then
            if v_del = true then
                pkg_cf.delete_field(v_cf_id);
            else
                v_app_label_id := v_cf.app_label_id;
                v_description_label_id := v_cf.description_label_id;

                if v_description is not null then
                    if v_description_label_id is not null then
                        update label_program lp1 set label_program_text = v_description
                         where label_program_id = v_description_label_id;
                    else
                        v_description_label_id := pkg_label.create_label_program(v_description);
                    end if;
                end if;
                if (p_line.comments is null) then
                    v_comments := v_cf.comments;
                end if;
                if (p_line.datatype is null) then
                    v_data_type := v_cf.data_type;
                end if;
                if (p_line.readonly is null) then
                    v_is_read_only := v_cf.is_read_only;
                end if;
                if (p_line.log_blob_changes is null) then
                    v_log_blob_changes := v_cf.log_blob_changes;
                end if;
                if (p_line.is_mandatory is null) then
                    v_is_mandatory := v_cf.is_mandatory;
                end if;
                if (p_line.is_two_cols_span is null) then
                    v_is_two_cols_span := v_cf.is_two_cols_span;
                end if;
                if (p_line.sql_query is null) then
                    v_sql_query := v_cf.sql_query;
                end if;
                if (p_line.is_lockable is null) then
                    v_is_lockable := v_cf.is_lockable;
                end if;
                if (p_line.use_in_my_things_filter is null) then
                    v_use_in_my_things_filter := v_cf.use_in_my_things_filter;
                end if;
                if p_line.show_expanded is null then
                    v_show_expanded := v_cf.show_expanded;
                end if;

                if v_cf_name is null and v_label_name is not null and p_line.field_id is not null then
                    --no action, new CFname = old CFname
                    v_cf_name := v_cf.config_field_name;
                end if;

                update config_field
                   set field_size = v_field_size,
                       config_field_name = v_cf_name,
                       lines_qty = v_lines_qty,
                       description_label_id = v_description_label_id,
                       comments = v_comments,
                       app_label_id = v_app_label_id,
                       attrib_v_table_id = v_table_id,
                       data_type = v_data_type,
                       field_width = v_field_width,
                       is_read_only = v_is_read_only,
                       log_blob_changes = v_log_blob_changes,
                       is_mandatory = v_is_mandatory,
                       is_two_cols_span = v_is_two_cols_span,
                       sql_query = v_sql_query,
                       is_lockable = v_is_lockable,
                       use_in_my_things_filter = v_use_in_my_things_filter,
                       show_expanded = v_show_expanded
                 where config_field_id = v_cf_id;

                if (v_label_name is not null) then
                    update label_program lp1 set label_program_text = v_label_name
                     where label_program_id = v_app_label_id
                       and label_program_text in (select label_program_text
                                                    from label_program
                                                   where label_program_id = v_app_label_id
                                                     and app_lang_id = pkg_sec.get_lang);
                else
                    raise_application_error(-20000, pkg_label.get_label_system(17662));
                end if;
            end if;
        else
            if v_data_type = pkg_cf.c_trackor_selector then
                v_obj_xitor_type_id := get_ttid_by_label_or_name(p_line.obj_xitor_type);
            end if;

            if (v_label_name is not null) then
                v_app_label_id := pkg_label.create_label_program(v_label_name);
            else
                raise_application_error(-20000, pkg_label.get_label_system(17665));
            end if;

            if (v_description is not null) then
                v_description_label_id := pkg_label.create_label_program(v_description);
            end if;

            if (v_cf_name is null) then
                v_cf_name := upper(rtrim(v_pref_lbl, ':')) || '_' || upper(regexp_replace(v_label_name, '\s+', '_'));
                v_cf_name := substr(regexp_replace(v_cf_name, '\W+', ''), 1, 30);
            end if;

            insert into config_field (field_size,
                lines_qty,
                description_label_id,
                comments,
                config_field_name,
                app_label_id,
                attrib_v_table_id,
                xitor_type_id,
                data_type,
                field_width,
                is_read_only,
                log_blob_changes,
                is_mandatory,
                is_two_cols_span,
                sql_query,
                obj_xitor_type_id,
                is_lockable,
                use_in_my_things_filter,
                show_expanded,
                is_maskable
            ) values (
                v_field_size,
                v_lines_qty,
                v_description_label_id,
                v_comments,
                v_cf_name,
                v_app_label_id,
                v_table_id,
                v_xitor_type_id,
                v_data_type,
                v_field_width,
                v_is_read_only,
                v_log_blob_changes,
                v_is_mandatory,
                v_is_two_cols_span,
                v_sql_query,
                v_obj_xitor_type_id,
                v_is_lockable,
                v_use_in_my_things_filter,
                v_show_expanded,
                v_is_maskable
            ) returning config_field_id into v_cf_id;
        end if;

        commit;

        return v_cf_id;
    end insert_cfield;

    procedure insert_config_group (
        p_line in cur_cf_line%rowtype,
        rid in imp_run.imp_run_id%type,
        p_cfid in config_field.config_field_id%type) 
    as
        pragma autonomous_transaction;
        v_cf_xitor_type_id xitor_type.xitor_type_id%type;
        v_tab_xitor_type_id xitor_type.xitor_type_id%type;
        v_tmp_xitor_type_id xitor_type.xitor_type_id%type;
        v_config_group_id config_group.config_group_id%type;
        v_tab_name config_group.description%type;
        v_tab_label label_program.label_program_text%type;
        v_config_element_id config_element.config_element_id%type;
        v_pid program.program_id%type;
    begin
        -- Determine v_tab_name based on p_line values
        if p_line.tab_name is null and p_line.tab_label is null then
            return;
        elsif p_line.tab_name is not null then
            v_tab_name := p_line.tab_name;
        else
            v_tab_name := pkg_str.gen_entity_name_for_label(p_line.tab_label, 50);
        end if;

        v_tab_label := coalesce(p_line.tab_label, v_tab_name);

        select program_id into v_pid from imp_run where imp_run_id = rid;

        -- Find field xitor_type_id
        select xitor_type_id
          into v_cf_xitor_type_id
          from config_field
         where config_field_id = p_cfid;

        -- Find tab xitor_type_id
        if    (p_line.tab_trackor_type is not null and lower(p_line.tab_trackor_type) = lower(p_line.component))
           or p_line.tab_trackor_type is null then
            v_tab_xitor_type_id := v_cf_xitor_type_id;
        else
            v_tab_xitor_type_id := get_ttid_by_label_or_name(p_line.tab_trackor_type);

            begin
                select xitor_type_id
                  into v_tmp_xitor_type_id
                  from (select column_value as xitor_type_id
                          from table(pkg_relation.get_parent_trackor_types_up_to_many_many_inclusively(v_tab_xitor_type_id))
                        union
                        select v_tab_xitor_type_id from dual)
                 where xitor_type_id = v_cf_xitor_type_id
                   and rownum = 1;

            exception
                when no_data_found then
                    raise_application_error(-20000, pkg_label.format(18415, pkg_label.list_label_params('cf_trackor_type' => p_line.component,
                                                                                                        'tab_trackor_type' => p_line.tab_trackor_type)));
            end;
        end if;

        -- Check if Config Group already exists
        begin
            select config_group_id
              into v_config_group_id
              from config_group cg
              join label_program l on cg.app_label_id = l.label_program_id 
                                  and l.app_lang_id = pkg_sec.get_lang
              where cg.program_id = v_pid
                and xitor_type_id = v_tab_xitor_type_id
                and (   lower(cg.description) = lower(v_tab_name) 
                     or lower(l.label_program_text) = lower(v_tab_label));
          exception
            when no_data_found then
                insert into config_group (description, xitor_type_id, program_id, app_label_id)
                values (v_tab_name, v_tab_xitor_type_id, v_pid,  pkg_label.create_label_program(v_tab_label))
                returning config_group_id into v_config_group_id;
        end;

        v_config_element_id := pkg_form.get_or_create_field_element(v_config_group_id, p_cfid);

        commit;
    end insert_config_group;

    procedure adjust_header_row(p_rid in imp_run.imp_run_id%type)
    is
        c_supported_property_names constant tableofchar := tableofchar('TRACKOR_TYPE','CONFIG_FIELD_NAME','CONFIG_FIELD_ID',
                                                                       'LABEL_TEXT','CONFIG_FIELD_ID','APP_LABEL_ID','DESCRIPTION',
                                                                       'DATA_TYPE','FIELD_SIZE','FIELD_WIDTH','LINES_QTY',
                                                                       'IS_READ_ONLY','TABLE_NAME','DELETE', 'SHOW_EXPANDED','IS_MASKABLE',
                                                                       'TAB_TRACKOR_TYPE', 'TAB_NAME', 'TAB_LABEL');

        v_1st_row_header_count integer;
        v_2nd_row_header_count integer;
    begin
        select count(*)
          into v_1st_row_header_count
          from imp_run_grid
         where imp_run_id = p_rid
           and row_num = (select start_row
                            from imp_run
                           where imp_run_id = p_rid)
           and to_char(data) member of c_supported_property_names;

        select count(*)
          into v_2nd_row_header_count
          from imp_run_grid
         where imp_run_id = p_rid
           and row_num = (select start_row + 1
                            from imp_run
                           where imp_run_id = p_rid)
           and to_char(data) member of c_supported_property_names;

        if (v_2nd_row_header_count >= v_1st_row_header_count ) then
            --header is in 2nd row, delete 1st row and shift row nums
            delete from imp_run_grid 
             where imp_run_id = p_rid 
               and row_num = (select start_row
                                from imp_run
                               where imp_run_id = p_rid);

            update imp_run_grid set row_num = row_num - 1 where imp_run_id = p_rid;
        end if;
    end adjust_header_row;

    function get_ttid_by_label_or_name(p_label_or_name in varchar2) return xitor_type.xitor_type_id%type
    as
        v_ttid xitor_type.xitor_type_id%type;
    begin
        begin
            select xt.xitor_type_id
              into v_ttid
              from xitor_type xt
              join label_program l on xt.applet_label_id = l.label_program_id 
                                  and l.app_lang_id = pkg_sec.get_lang
             where xt.program_id = pkg_sec.get_pid
               and (   lower(xt.xitor_type) = lower(p_label_or_name)
                    or lower(l.label_program_text) = lower(p_label_or_name));

        exception
            when no_data_found then
                raise_application_error(-20000, pkg_label.format(17663, pkg_label.list_label_params('trackor_name' => upper(p_label_or_name))));
            when too_many_rows then
                raise_application_error(-20000, pkg_label.format(17664, pkg_label.list_label_params('trackor_name' => upper(p_label_or_name))));
        end;

        return v_ttid;
    end get_ttid_by_label_or_name;
end pkg_ext_imp_field;
/