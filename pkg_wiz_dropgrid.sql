CREATE OR REPLACE PACKAGE BODY PKG_WIZ_DROPGRID 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

    /*DECLARATION OF PRIVET API*/
    procedure create_relation_imp_mapping(
        p_imp_id imp_spec.imp_spec_id%type,
        p_imp_entity_id imp_entity.imp_entity_id%type,
        p_parent_ttid xitor_type.xitor_type_id%type,
        p_imp_col_id imp_column.imp_column_id%type);
    procedure create_relation_imp_mappings(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_imp_id imp_spec.imp_spec_id%type,
        p_imp_entity_id imp_entity.imp_entity_id%type);
    function create_imp_entity(
        p_imp_id imp_spec.imp_spec_id%type,
        p_ttid xitor_type.xitor_type_id%type,
        p_type_lbl_id xitor_type.applet_label_id%type,
        p_imp_col_id imp_column.imp_column_id%type,
        p_ord_num imp_entity.order_number%type)
        return imp_entity.imp_entity_id%type;
    function get_unique_table_name(p_name varchar2) return varchar2;
    function get_prim_field_label(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) return varchar2;
    function get_trackor_class_label(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) return varchar2;
    function create_trackor_type_int(
                p_name xitor_type.xitor_type%type,
                p_label varchar2,
                p_prefix varchar2,
                p_prim_key_label varchar2,
                p_trackor_class_label varchar2,
                p_pid program.program_id%type) return xitor_type.xitor_type_id%type;
    procedure assign_fields_to_tab(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_cgid config_group.config_group_id%type);
    function encode_cf(p_cfid config_field.config_field_id%type)
        return varchar2;
    procedure assign_fields_to_view(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_vid view_opt.view_opt_id%type,
        p_module_name grid_page.module_name%type);
    function remove_invalid_chars_in_name(p_name varchar2) return varchar2;
    function get_unique_trackor_type_name(
      p_name xitor_type.xitor_type%type,
      p_pid program.program_id%type) return xitor_type.xitor_type%type;

    /*PUBLIC API*/
    procedure create_trackor_type(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_ttid xitor_type.xitor_type_id%type;
        v_trackor_type_name xitor_type.xitor_type%type;
        v_prim_field_label varchar2(255);
        v_trackor_class_label varchar2(255);
        v_type_name_length number;
        v_sheet_id dropgrid_sheet.dropgrid_sheet_id%type;
        v_new_trackor_type dropgrid_sheet.new_trackor_type%type;
        v_new_prefix dropgrid_sheet.new_prefix%type;
        v_pid dropgrid_sheet.program_id%type;
    begin
        select dropgrid_sheet_id,
               new_trackor_type,
               new_prefix,
               program_id
          into v_sheet_id,
               v_new_trackor_type,
               v_new_prefix,
               v_pid
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        set_status(v_sheet_id, c_trackor_type_status);

        v_trackor_type_name := remove_invalid_chars_in_name(v_new_trackor_type);

        --Trackor Type name is used in names of master applet, trackor container and master tab
        select min(column_value)
          into v_type_name_length
          from table(new tableofnum(c_max_applet_name_length, c_max_tab_desc_length, c_max_trackor_type_length));

        --Master Tab name is Trackor Type name plus postfilx.
        --We should correct max Trackor Type name according to length of the postfix
        v_type_name_length := v_type_name_length - length(pkg_const.c_general_tab_postfix);

        v_trackor_type_name := substr(v_trackor_type_name, 1, v_type_name_length);
        v_trackor_type_name := get_unique_trackor_type_name(v_trackor_type_name, v_pid);
        v_prim_field_label := get_prim_field_label(v_sheet_id);
        v_trackor_class_label := get_trackor_class_label(v_sheet_id);

        v_ttid := create_trackor_type_int(
                v_trackor_type_name,
                v_new_trackor_type,
                v_new_prefix,
                v_prim_field_label,
                v_trackor_class_label,
                v_pid);

        update dropgrid_sheet
           set trackor_type_id = v_ttid
         where dropgrid_sheet_id = v_sheet_id;
    end create_trackor_type;

    procedure set_trackor_key_gen_formula(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as
        v_formula varchar2(500);
        v_start_at dropgrid_sheet.new_key_start_at%type;
        v_ttid dropgrid_sheet.trackor_type_id%type;
        v_new_ttid dropgrid_sheet.new_trackor_type_id%type;
        v_is_autokey xitor_type.is_autokey%type;
    begin
        select coalesce(to_char(s.new_key_config_field_id1), to_char(f1.config_field_id), '') 
               || '/' 
               || nvl(s.new_key_delim1,'') 
               || '|' 
               || coalesce(to_char(s.new_key_config_field_id2), to_char(f2.config_field_id), '') 
               || '/' 
               || nvl(s.new_key_delim2,'') 
               || '|' 
               || nvl(s.new_key_string_pref3,'') 
               || '/' 
               || nvl(s.new_key_delim3,'') 
               || '|' 
               || nvl(s.new_key_suffix, 6) 
               || '/'
               || nvl(s.new_key_suffix_across, '') as formula,
               s.new_key_start_at,
               s.new_trackor_type_id,
               s.trackor_type_id,
               xt.is_autokey
          into v_formula,
               v_start_at,
               v_new_ttid,
               v_ttid,
               v_is_autokey
          from dropgrid_sheet s
          join xitor_type xt on (xt.xitor_type_id = s.trackor_type_id)
          left join dropgrid_field f1 on (f1.dropgrid_field_id = s.new_key_dropgrid_field_id1)
          left join dropgrid_field f2 on (f2.dropgrid_field_id = s.new_key_dropgrid_field_id2)
         where s.dropgrid_sheet_id = p_sheet_id;

        if v_new_ttid > 0 and (v_start_at is null or v_is_autokey = 0) then
            --Update Trackor and autokey formula is not used (v_is_autokey = 0) or was not modified (v_start_at is null)
            return;
        elsif v_new_ttid = 0 and v_start_at is null then
            -- Create Trackor and autokey generation is not used
            pkg_xitor_type.create_trackor_sequence(v_ttid);
        else
            --Update Trackor and modify autokey formula or Create Trackor with autokey generation
            update xitor_type
               set is_autokey = 1,
                   autokey_formula = v_formula,
                   autokey_start_at = v_start_at
             where xitor_type_id = v_ttid;

            pkg_xitor_type.create_trackor_sequence(v_ttid, v_start_at);
        end if;

    end set_trackor_key_gen_formula;

    procedure update_trackor_type(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as
    begin
        set_status(p_sheet_id, c_trackor_type_status);

        update dropgrid_sheet set
            trackor_type_id = new_trackor_type_id,
            is_trackor_update = 1
        where dropgrid_sheet_id = p_sheet_id;
    end update_trackor_type;

    procedure create_rel_types(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_is_trackor_update dropgrid_sheet.is_trackor_update%type;
        v_rel_type_number number;
        v_rtid relation_type.relation_type_id%type;
        v_sec_group_id sec_group_program.sec_group_program_id%type;
        v_ttid dropgrid_sheet.trackor_type_id%type;
        v_pid dropgrid_sheet.program_id%type;
    begin
        set_status(p_sheet_id, c_relation_type_status);

        select is_trackor_update,
               trackor_type_id,
               program_id
          into v_is_trackor_update,
               v_ttid,
               v_pid
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        for rec in (select new_parent_ttid as parent_ttid,
                           order_number
                      from dropgrid_field
                     where dropgrid_sheet_id = p_sheet_id
                       and (new_imp_col = 1 and nvl(new_parent_ttid,0) > 0)
                       and v_is_trackor_update = 0)
        loop
            begin
                select count(relation_type_id)
                  into v_rel_type_number
                  from relation_type
                 where parent_type_id = rec.parent_ttid
                   and child_type_id = v_ttid;
            exception 
                when no_data_found 
                    then v_rel_type_number := 0;
            end;

            if (v_rel_type_number = 0) then
                v_rtid := pkg_relation.new_relation_type(
                    rec.parent_ttid,
                    v_ttid,
                    c_one2many_rel_cardinality,
                    0,
                    0,
                    null);
                update dropgrid_field
                   set relation_type_id = v_rtid
                 where dropgrid_sheet_id = p_sheet_id
                   and order_number = rec.order_number;

                if pkg_sec.get_cu() is not null then
                    --Assign tab to an user
                    select sec_group_program_id
                      into v_sec_group_id
                      from sec_group_program
                     where relation_type_id = v_rtid
                       and sec_group_type_id = c_relation_sec_gr_type_id
                       and program_id = v_pid;

                    pkg_sec_priv_program.user_sec_exception_upsert(pkg_sec.get_cu(), v_sec_group_id, 'READ');
                end if;
            end if;
        end loop;

        if (v_is_trackor_update = 0 and v_rtid is null) then	
            --Assign Trackor Type to root if parental Trackor Types are not defined	
            v_rtid := pkg_relation.new_relation_type(	
                null,	
                v_ttid,	
                c_one2many_rel_cardinality,	
                0,	
                0,	
                null);	
        end if;
    end create_rel_types;

    procedure create_menu(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_module_name grid_page.module_name%type,
        p_dg_menu_item_ids tableofnum) as

        v_menu_label label_program.label_program_text%type;
        v_menu_id menu_items.menu_item_id%type;
        v_menu_gpid grid_page.grid_page_id%type;
        v_menu_prim_ttid xitor_type.xitor_type_id%type;
        v_menu_url menu_items.url%type := null;
        v_pid dropgrid_sheet.program_id%type;
        v_ttid dropgrid_sheet.trackor_type_id%type;
        v_menu_xref_id menu_items_app_xref.menu_items_app_xref_id%type;
    begin
        select ds.program_id,
               ds.trackor_type_id,
               pkg_label.get_label_program(xt.applet_label_id, pkg_sec.get_lang())
          into v_pid, 
               v_ttid,
               v_menu_label
          from dropgrid_sheet ds
          join xitor_type xt on (xt.xitor_type_id = ds.trackor_type_id)
         where ds.dropgrid_sheet_id = p_sheet_id;

        if (p_module_name = c_map_module_name) then
            set_status(p_sheet_id, c_map_menu_status);
        else
            set_status(p_sheet_id, c_tb_menu_status);
        end if;

        if (p_module_name = c_map_module_name) then
            v_menu_label := v_menu_label || ' (' || pkg_label.get_label_system(826, pkg_sec.get_lang()) || ')';
        end if;

        select grid_page_id
          into v_menu_gpid
          from grid_page
         where module_name = p_module_name;

        if (p_module_name = c_tb_module_name or p_module_name = c_map_module_name) then
            v_menu_prim_ttid := v_ttid;
        end if;

        if (p_dg_menu_item_ids is not null) then
            for rec in (select application_id,
                               menu_item_id
                          from menu_items_app_xref
                         where menu_item_id in (select column_value from table(p_dg_menu_item_ids)))
            loop
                insert into menu_items (
                    label_id,
                    grid_page_id,
                    primary_xitor_type_id,
                    url,
                    program_id
                ) values (
                    pkg_label.create_label_program(v_menu_label),
                    v_menu_gpid,
                    v_menu_prim_ttid,
                    v_menu_url,
                    v_pid
                ) returning menu_item_id
                       into v_menu_id;

                insert into menu_items_app_xref (
                    application_id,
                    is_visible,
                    menu_item_id,
                    parent_menu_item_id,
                    order_number,
                    program_id
                ) values (
                    rec.application_id,
                    1,
                    v_menu_id,
                    rec.menu_item_id,
                    v_menu_id,
                    v_pid
                ) returning menu_items_app_xref_id
                       into v_menu_xref_id;

                insert into dg_sheet_menu_xref (
                    dropgrid_sheet_id,
                    menu_item_id,
                    menu_items_app_xref_id,
                    program_id
                ) values (
                    p_sheet_id,
                    v_menu_id,
                    v_menu_xref_id,
                    v_pid
                );
            end loop;
        end if;
    end create_menu;

    procedure create_config_fields(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_cfid config_field.config_field_id%type;
        v_table_id attrib_v_table.attrib_v_table_id%type;
        v_table_name attrib_v_table.attrib_v_table_name%type;
        v_lat_field_number number := 0;
        v_lat_cfid config_field.config_field_id%type;
        v_long_field_number number := 0;
        v_long_cfid config_field.config_field_id%type;
        v_sheet_rec dropgrid_sheet%rowtype;
        v_coord_count number;
    begin
        select * into v_sheet_rec
        from dropgrid_sheet where dropgrid_sheet_id = p_sheet_id;

        set_status(p_sheet_id, c_config_field_status);

         --set DROPGRID_FIELD.CONFIG_FIELD_ID for parental fields
        for rec in (
            select f.order_number,
                   cf.config_field_id,
                   cf.data_type
              from dropgrid_field f
              join config_field cf on (cf.config_field_name = f.new_field_name and cf.xitor_type_id = nvl(f.new_parent_ttid,0))
             where f.dropgrid_sheet_id = p_sheet_id
               and (cf.is_static = 0 or cf.config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id))
        ) loop
            update dropgrid_field set
                config_field_id = rec.config_field_id,
                is_import_column = 1,
                is_created = 0,
                is_primary_key = 0,
                parent_ttid = new_parent_ttid,
                obj_trackor_type_id = null,
                data_type_id = 0
            where dropgrid_sheet_id = p_sheet_id
                and order_number = rec.order_number;
        end loop;

        --set DROPGRID_FIELD.CONFIG_FIELD_ID for existent config fields
        for rec in (
            select f.order_number, cf.config_field_id, cf.data_type
              from dropgrid_field f
              join config_field cf on (cf.config_field_name = f.new_field_name)
             where f.dropgrid_sheet_id = p_sheet_id
               and ((new_imp_col = 1 and nvl(new_prim_key,0) = 0 and nvl(new_parent_ttid,0) = 0) or (nvl(new_prim_key,0) = 1))
               and nvl(f.new_data_type_id, -1) <> -1
               and (cf.is_static = 0 or cf.config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id))
               and cf.xitor_type_id = v_sheet_rec.trackor_type_id
        ) loop
            update dropgrid_field set
                config_field_id = rec.config_field_id,
                is_import_column = 1,
                is_created = 0,
                is_primary_key = nvl(new_prim_key,0),
                parent_ttid = null,
                obj_trackor_type_id = new_obj_ttid,
                data_type_id = rec.data_type
            where dropgrid_sheet_id = p_sheet_id
                and order_number = rec.order_number;

            if (rec.data_type = c_latitude_field_type) then
                v_lat_field_number := v_lat_field_number + 1;
                v_lat_cfid := rec.config_field_id;
            elsif (rec.data_type = c_longitude_field_type) then
                v_long_field_number := v_long_field_number + 1;
                v_long_cfid := rec.config_field_id;
            end if;
        end loop;

        --create new config fields on the base DropGrid fields
        for rec in (
            select new_field_name, new_field_label, new_data_type_id, new_obj_ttid, order_number
            from dropgrid_field
            where
                new_imp_col = 1
                and nvl(new_parent_ttid,0) = 0
                and nvl(new_prim_key,0) = 0
                and nvl(new_data_type_id, -1) <> -1
                and config_field_id is null
                and dropgrid_sheet_id = p_sheet_id
        ) loop
            insert into config_field (
                config_field_name
                ,app_label_id
                ,data_type
                ,field_width
                ,field_size
                ,xitor_type_id
                ,is_two_cols_span
                ,lines_qty
                ,log_blob_changes
                ,obj_xitor_type_id
                ,program_id
            ) values (
                rec.new_field_name
                ,pkg_label.create_label_program(rec.new_field_label)
                ,rec.new_data_type_id
                ,decode(rec.new_data_type_id, c_memo_field_type, c_memo_field_width, c_def_field_width)
                ,decode(rec.new_data_type_id,
                    c_date_field_type, c_date_field_type,
                    c_datetime_field_type, c_datetime_field_size,
                    c_time_field_type, c_time_field_size,
                    c_latitude_field_type, c_latlong_field_size,
                    c_longitude_field_type, c_latlong_field_size,
                    c_number_field_type, c_num_field_size,
                    c_trackor_sel_field_type, c_trackor_field_size,
                    c_trackor_drop_field_type, c_trackor_field_size,
                    c_def_field_size
                )
                ,v_sheet_rec.trackor_type_id
                ,decode(rec.new_data_type_id, c_memo_field_type, 1, 0)
                ,decode(rec.new_data_type_id,
                    c_memo_field_type, 5,
                    c_multiselector_field_type, 2,
                    null
                )
                ,decode(rec.new_data_type_id, c_efile_field_type, 1, 0)
                ,decode(rec.new_data_type_id,
                  c_trackor_sel_field_type, rec.new_obj_ttid,
                  c_trackor_drop_field_type, rec.new_obj_ttid,
                  null
                )
                ,v_sheet_rec.program_id
            ) returning config_field_id into v_cfid;

            if (rec.new_data_type_id = c_selector_field_type
                or rec.new_data_type_id = c_drop_down_field_type
                or rec.new_data_type_id = c_multiselector_field_type) then
                v_table_name := get_unique_table_name(c_vtable_name_prefix || rec.new_field_name);
                insert into attrib_v_table (
                    attrib_v_table_name,
                    program_id
                ) values (
                    v_table_name,
                    v_sheet_rec.program_id
                ) returning attrib_v_table_id into v_table_id;
                update config_field set attrib_v_table_id = v_table_id
                where config_field_id = v_cfid;
            end if;

            update dropgrid_field set config_field_id = v_cfid, is_import_column = 1, is_created = 1
            where dropgrid_sheet_id = p_sheet_id and order_number = rec.order_number;

            if (rec.new_data_type_id = c_latitude_field_type) then
                v_lat_field_number := v_lat_field_number + 1;
                v_lat_cfid := v_cfid;
            elsif (rec.new_data_type_id = c_longitude_field_type) then
                v_long_field_number := v_long_field_number + 1;
                v_long_cfid := v_cfid;
            end if;
        end loop;

        if (v_lat_field_number = 1 and v_long_field_number = 1) then
            /*Create Lat/Long pair if do not exist*/
            select count(1) into v_coord_count
              from config_field_coord_link
             where latitude_field_id = v_lat_cfid
               and longitude_field_id = v_long_cfid;

            if v_coord_count = 0 then
                insert into config_field_coord_link (
                    name,
                    trackor_type_id,
                    latitude_field_id,
                    longitude_field_id,
                    program_id
                ) values (
                    c_cf_coord_link_name_prefix || v_sheet_rec.dropgrid_sheet_id,
                    v_sheet_rec.trackor_type_id,
                    v_lat_cfid,
                    v_long_cfid,
                    v_sheet_rec.program_id);
            end if;
        end if;
    end create_config_fields;

    procedure create_import(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_imp_id imp_spec.imp_spec_id%type;
        v_dgid dropgrid.dropgrid_id%type;
        v_date_format users.date_format%type;
        v_time_format imp_spec.time_format%type;
        v_line_delim_id v_line_delimiters.line_delimiter_id%type;
        v_str_quote_id v_string_quote.string_quote_id%type;
        v_field_delim_id v_delimiters.delimiter_id%type;
        v_imp_col_id imp_column.imp_column_id%type;
        v_prim_key_imp_entity_id imp_entity.imp_entity_id%type;
        v_prim_ttid xitor_type.xitor_type_id%type;
        v_prim_type_lbl_id xitor_type.applet_label_id%type;
        v_pid program.program_id%type;
        v_imp_name imp_spec.name%type;
    begin
        set_status(p_sheet_id, c_import_status);

        select s.dropgrid_id,
               s.trackor_type_id,
               xt.applet_label_id,
               s.program_id,
               new_import_name
          into v_dgid,
               v_prim_ttid,
               v_prim_type_lbl_id,
               v_pid,
               v_imp_name
          from dropgrid_sheet s
          join xitor_type xt on (xt.xitor_type_id = s.trackor_type_id)
         where dropgrid_sheet_id = p_sheet_id;

        select date_format,
               case when lower(time_format) = 'hh:mm:ss aa' then 'HH12:MI:SS PM'
                    else 'HH24:MI:SS'
               end
          into v_date_format,
               v_time_format
          from users
         where user_id = pkg_sec.get_cu();

        select line_delimiter_id
          into v_line_delim_id
          from v_line_delimiters
         where delimiter = c_imp_def_line_delim;

        select string_quote_id
          into v_str_quote_id
          from v_string_quote
         where quote = c_imp_def_str_quote;

        select delimiter_id
          into v_field_delim_id
          from v_delimiters
         where delimiter = c_imp_def_field_delim;

        --Create Imp Spec
        insert into imp_spec (
            imp_file_type_id,
            name,
            description,
            date_format,
            time_format,
            log_warnings_in_comments,
            line_delimiter_id,
            string_quote_id,
            field_delimiter_id,
            imp_action,
            default_action,
            validate_data_mode_id,
            program_id,
            external_proc
        ) values (
            c_imp_csv_file_type,
            v_imp_name,
            pkg_label.get_label_system(6136, pkg_sec.get_lang()),
            v_date_format,
            v_time_format,
            1,
            v_line_delim_id,
            v_str_quote_id,
            v_field_delim_id,
            to_char(c_imp_def_action_id),
            c_imp_def_action_id,
            c_imp_no_validate_mode,
            v_pid,
            'pkg_dl_support.AllowNulls := 1; pkg_dl_support.CreateNewValue := true; pkg_ext_imp.XitorConfiguredFieldLoad(:rid, ' || v_prim_ttid || ', 1, 1);'
        ) returning imp_spec_id
               into v_imp_id;

        --Create Import Columns
         for rec in (
            select field_label,
                   (2 * rownum) as ord_num,
                   parent_ttid,
                   parent_lbl_id,
                   prim_key,
                   cfid
              from (select case when (new_prim_key = 1 or (new_parent_ttid is not null and new_parent_ttid > 0)) and unique_lbl_rn = 1 then field_label
                                when (new_prim_key = 1 or (new_parent_ttid is not null and new_parent_ttid > 0)) and unique_lbl_rn > 1 then field_label || ' (' || unique_lbl_rn || ')'
                                else new_field_name
                           end as field_label,
                           new_parent_ttid as parent_ttid,
                           applet_label_id as parent_lbl_id,
                           new_prim_key as prim_key,
                           config_field_id as cfid
                      from (select f.new_parent_ttid,
                                   xt.applet_label_id,
                                   f.field_label,
                                   f.new_field_name,
                                   f.new_prim_key,
                                   f.order_number,
                                   row_number() over (partition by f.field_label order by f.order_number) as unique_lbl_rn,
                                   row_number() over (partition by decode(nvl(f.new_parent_ttid,0), 0, 1, 0) order by f.order_number) as parent_rn,
                                   row_number() over (partition by decode(nvl(f.new_parent_ttid,0), 0, decode(nvl(f.new_prim_key,0), 0, 0, 1), 1) order by f.order_number) as field_rn,
                                   decode(f.new_parent_ttid, null, decode(nvl(f.new_prim_key,0), 0, 2, 1), 0) as group_id,
                                   cf.config_field_id
                              from dropgrid_field f
                              join config_field cf on (cf.config_field_id = f.config_field_id)
                              left join xitor_type xt on (xt.xitor_type_id = f.new_parent_ttid)
                             where f.dropgrid_sheet_id = p_sheet_id
                               and f.new_data_type_id <> c_efile_field_type)
                     order by group_id asc,
                              case when nvl(new_parent_ttid,0) = 0 then
                                        (case when nvl(new_prim_key,0) = 0 then field_rn
                                             else 1
                                         end)
                                   else parent_rn
                              end asc))
        loop
            if rec.parent_ttid is not null or rec.prim_key = 1 then
                insert into imp_column (
                    imp_spec_id,
                    name,
                    order_number,
                    program_id
                ) values (
                    v_imp_id,
                    rec.field_label,
                    rec.ord_num,
                    v_pid
                ) returning imp_column_id into v_imp_col_id;
            end if;

            if (rec.prim_key = 1) then
                v_prim_key_imp_entity_id := create_imp_entity(v_imp_id, v_prim_ttid, v_prim_type_lbl_id, v_imp_col_id, rec.ord_num);
            end if;

        end loop;

        if v_prim_key_imp_entity_id is null then
            --Trackor Type with key generation
            v_prim_key_imp_entity_id := create_imp_entity(v_imp_id, v_prim_ttid, v_prim_type_lbl_id, null, 0);
        end if;

        create_relation_imp_mappings(
                    p_sheet_id,
                    v_imp_id,
                    v_prim_key_imp_entity_id);

        update dropgrid_sheet
           set imp_spec_id = v_imp_id
         where dropgrid_sheet_id = p_sheet_id;

        create_import_process(p_sheet_id);
    end create_import;

    procedure create_import_process(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as
        v_load_data dropgrid_sheet.new_load_data%type;
        v_imp_spec_id dropgrid_sheet.imp_spec_id%type;
        v_pid dropgrid_sheet.program_id%type;
        v_new_imp_spec_id dropgrid_sheet.new_imp_spec_id%type;
        v_imp_proc_id process.process_id%type;
    begin
        set_status(p_sheet_id, c_import_status);

        select new_imp_spec_id,
               imp_spec_id,
               nvl(new_load_data,0)
          into v_new_imp_spec_id,
               v_imp_spec_id,
               v_load_data
          from dropgrid_sheet s
         where dropgrid_sheet_id = p_sheet_id;

        if v_load_data = 1 then
            /*Create Import Process*/
            insert into process (
                user_id,
                submission_date,
                start_date,
                status_id,
                process_type_id,
                program_id
            ) values (
                pkg_sec.get_cu(),
                current_date,
                current_date,
                c_imp_pending_proc_status_id,
                c_imp_process_type_id,
                v_pid
            ) returning process_id into v_imp_proc_id;

            insert into imp_run(
                process_id,
                imp_spec_id,
                imp_action_id,
                notify_on_completion,
                is_incremental,
                program_id)
            values (
                v_imp_proc_id,
                case when v_new_imp_spec_id = 0
                     then v_imp_spec_id
                     else v_new_imp_spec_id
                end,
                c_imp_insert_update_action_id,
                0,
                0,
                v_pid);
        end if;

        update dropgrid_sheet
           set import_process_id = v_imp_proc_id
         where dropgrid_sheet_id = p_sheet_id;
    end create_import_process;

    procedure create_config_tab(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_sheet_rec dropgrid_sheet%rowtype;
        v_cgid config_group.config_group_id%type;
        v_lbl label_program.label_program_text%type;
        v_desc config_group.description%type;
        v_sec_group_id sec_group_program.sec_group_program_id%type;
        v_capp_id config_app.config_app_id%type;
        v_tab_app_xref_id config_app_group_xref.config_app_group_xref_id%type;
        v_name config_group.description%type;
        v_name_prefix label_system.label_system_text%type;
    begin
        set_status(p_sheet_id, c_config_tab_status);

        select *
          into v_sheet_rec
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        v_lbl := nvl(v_sheet_rec.new_group_lbl, v_sheet_rec.sheet_name);
        v_name_prefix := pkg_label.get_label_system(6225, pkg_sec.get_lang());
        v_name := substr(v_lbl, 1, c_max_tab_desc_length - length(v_name_prefix));
        insert into config_group (
            description,
            app_label_id,
            xitor_type_id,
            static_id,
            program_id
        ) values (
            v_name_prefix || v_name,
            pkg_label.create_label_program(v_lbl),
            v_sheet_rec.trackor_type_id,
            c_dynamic_tab_static_id,
            v_sheet_rec.program_id
        ) returning config_group_id,
                    description
               into v_cgid,
                    v_desc;

        update dropgrid_sheet
           set config_group_id = v_cgid
         where dropgrid_sheet_id = p_sheet_id;

        --Assign tab to an user
        select sec_group_program_id
          into v_sec_group_id
          from sec_group_program
         where security_group = v_desc
           and sec_group_type_id = c_config_tab_sec_gr_type_id
           and program_id = v_sheet_rec.program_id;

        pkg_sec_priv_program.user_sec_exception_upsert(pkg_sec.get_cu(), v_sec_group_id, 'RE');

        /*Assign tab to Trackor Class*/
        for rec in (select xitor_class_id
                      from v_xitor_class
                     where xitor_type_id = v_sheet_rec.trackor_type_id)
        loop
            insert into config_group_xitor_class_xref (
                config_group_id,
                xitor_class_id,
                program_id
            ) values (
                v_cgid,
                rec.xitor_class_id,
                v_sheet_rec.program_id
            );
        end loop;

        /*Assign the Tab to the Master Applet*/
        select config_app_id
          into v_capp_id
          from config_app
         where xitor_type_id = v_sheet_rec.trackor_type_id
           and is_master_app = 1;
        v_tab_app_xref_id := pkg_cf.append_tab_to_app(v_capp_id, v_cgid, null);

        assign_fields_to_tab(v_sheet_rec.dropgrid_sheet_id, v_cgid);
    end create_config_tab;

    procedure update_config_tab(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_new_cgid dropgrid_sheet.new_cgid%type;
        v_ttid dropgrid_sheet.trackor_type_id%type;
        v_pid dropgrid_sheet.program_id%type;
        v_desc config_group.description%type;
        v_sec_group_id sec_group_program.sec_group_program_id%type;
    begin
        set_status(p_sheet_id, c_config_tab_status);

        select new_cgid,
               trackor_type_id,
               program_id
          into v_new_cgid,
               v_ttid,
               v_pid
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        if v_new_cgid = c_do_not_modify_value_id then
            return;
        end if;

        if (v_new_cgid = 1) then
            select config_group_id
              into v_new_cgid
              from config_group
             where xitor_type_id = v_ttid
               and is_master_tab = 1;
        end if;

        update dropgrid_sheet
           set is_group_update = 1,
               config_group_id = v_new_cgid
         where dropgrid_sheet_id = p_sheet_id;

        select description
          into v_desc
          from config_group
         where config_group_id = v_new_cgid;

        assign_fields_to_tab(p_sheet_id, v_new_cgid);

        --Assign tab to an user
        select sec_group_program_id
          into v_sec_group_id
          from sec_group_program
         where security_group = v_desc
           and sec_group_type_id = c_config_tab_sec_gr_type_id
           and program_id = v_pid;

        pkg_sec_priv_program.user_sec_exception_upsert(pkg_sec.get_cu(), v_sec_group_id, 'RE');
    end update_config_tab;

    procedure create_view(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_module_name grid_page.module_name%type,
        p_uid users.user_id%type) as

        v_vid view_opt.view_opt_id%type;
        v_gpid grid_page.grid_page_id%type;
        v_sheet_rec dropgrid_sheet%rowtype;
        v_view_map_id view_opt_map.view_opt_map_id%type;
        v_max_info_cols number;
        v_field_code varchar2(50);
        v_lat_cf_label dropgrid_field.new_field_label%type;
        v_long_cf_label dropgrid_field.new_field_label%type;
        v_is_config_map_page number := 0;
        v_view_name view_opt.name%type;
        v_view_exs_counter number := 2;
        v_view_name_suffix view_opt.name%type := '';
        v_lat_cf_pair_link_id config_field_coord_link.config_field_coord_link_id%type;
        v_long_cf_pair_link_id config_field_coord_link.config_field_coord_link_id%type;
    begin
        select *
          into v_sheet_rec
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        if (p_module_name = c_map_module_name) then
            if (p_uid is not null) then
                set_status(p_sheet_id, c_map_def_view_status);
            else
                set_status(p_sheet_id, c_map_view_status);
            end if;
            v_is_config_map_page := 1;
        else
            if (p_uid is not null) then
                set_status(p_sheet_id, c_tb_def_view_status);
            else
                set_status(p_sheet_id, c_tb_view_status);
            end if;
        end if;

        select grid_page_id
          into v_gpid
          from grid_page
         where module_name = p_module_name;

        loop
          begin
              insert into view_opt (
                  grid_page_id,
                  trackor_type_id,
                  user_id,
                  name,
                  program_id,
                  filter_view_type_id
              ) values (
                  v_gpid,
                  v_sheet_rec.trackor_type_id,
                  p_uid,
                  decode(p_uid, null, nvl(v_sheet_rec.new_view_lbl, v_sheet_rec.sheet_name) || v_view_name_suffix, null),
                  v_sheet_rec.program_id,
                  pkg_filter_opt.c_grid_page_filter_type_id
              ) returning view_opt_id,
                          name
                     into v_vid,
                          v_view_name;
          exception
              when dup_val_on_index then
                  v_vid := null;
                  if p_uid is null then
                    v_view_name_suffix := ' (' || v_view_exs_counter || ')';
                    v_view_exs_counter := v_view_exs_counter + 1;
                  end if;
          end;
        exit when (v_vid is not null and p_uid is null) or (v_vid is null and p_uid is not null);
        end loop;

        if (v_vid is null) then
            return;
        end if;

        update dropgrid_sheet
           set new_view_lbl = v_view_name
         where dropgrid_sheet_id = v_sheet_rec.dropgrid_sheet_id;

        if (p_uid is null) then
            /*Save view as current*/
            delete from view_opt_current c
             where c.user_id = pkg_sec.get_cu()
               and c.master_ttid is null
               and c.grid_page_id = v_gpid
               and exists (select vo.view_opt_id
                             from view_opt vo
                            where vo.view_opt_id = c.view_opt_id
                              and vo.trackor_type_id = v_sheet_rec.trackor_type_id);

            insert into view_opt_current (
                user_id,
                view_opt_id,
                program_id,
                grid_page_id
            ) values (
                pkg_sec.get_cu(),
                v_vid,
                v_sheet_rec.program_id,
                v_gpid
            );

            if (p_module_name = c_map_module_name) then
                update dropgrid_sheet
                   set map_view_opt_id = v_vid
                 where dropgrid_sheet_id = v_sheet_rec.dropgrid_sheet_id;
            elsif (p_module_name = c_tb_module_name) then
                update dropgrid_sheet
                   set tb_view_opt_id = v_vid
                 where dropgrid_sheet_id = v_sheet_rec.dropgrid_sheet_id;
            end if;

            update dropgrid_sheet
               set is_configured_map_page = v_is_config_map_page
             where dropgrid_sheet_id = v_sheet_rec.dropgrid_sheet_id;

            pkg_user.user_obj_exception_modify(pkg_sec.get_cu(),
                v_vid,
                c_view_sec_gr_type_id,
                1);

            assign_fields_to_view(v_sheet_rec.dropgrid_sheet_id, v_vid, p_module_name);

            if (p_module_name = c_map_module_name) then
                select f.new_field_label,
                       l.config_field_coord_link_id
                  into v_lat_cf_label,
                       v_lat_cf_pair_link_id
                  from dropgrid_field f
                  join config_field cf on (cf.config_field_id = f.config_field_id)
                  left join config_field_coord_link l on latitude_field_id = cf.config_field_id
                 where f.dropgrid_sheet_id = p_sheet_id
                   and cf.data_type = c_latitude_field_type;

                select f.new_field_label,
                       l.config_field_coord_link_id
                  into v_long_cf_label,
                       v_long_cf_pair_link_id
                  from dropgrid_field f
                  join config_field cf on (cf.config_field_id = f.config_field_id)
                  left join config_field_coord_link l on longitude_field_id = cf.config_field_id
                 where f.dropgrid_sheet_id = p_sheet_id
                   and cf.data_type = c_longitude_field_type;

                if v_lat_cf_pair_link_id is null
                   or v_long_cf_pair_link_id is null
                   or v_lat_cf_pair_link_id <> v_long_cf_pair_link_id then

                   raise_application_error(-20000, pkg_label.format(6597, pkg_label.list_label_params('latfield' => v_lat_cf_label, 
                                                                                                      'longfield' => v_long_cf_label)));

                end if;

                insert into view_opt_map (
                    view_opt_id,
                    trackor_type_id,
                    shape_id,
                    is_show_trackors,
                    symbol_color,
                    symbol_code,
                    config_field_coord_link_id,
                    program_id
                ) values (
                    v_vid,
                    v_sheet_rec.trackor_type_id,
                    c_map_symbol_id,
                    1,
                    c_red_color_hex,
                    c_map_symbol_code,
                    v_lat_cf_pair_link_id,
                    v_sheet_rec.program_id
                ) returning view_opt_map_id
                       into v_view_map_id;

                v_max_info_cols := pkg_const.get_param_program_val('MapperMaxInfoCols', v_sheet_rec.program_id);
                for rec in (
                    select d.*,
                           rownum as order_number
                      from (select cf.*
                              from dropgrid_field f
                              join config_field cf on (cf.config_field_id = f.config_field_id)
                             where f.dropgrid_sheet_id = p_sheet_id
                             order by case when f.is_primary_key = 1 then -3
                                           when f.data_type_id = c_latitude_field_type then -2
                                           when f.data_type_id = c_longitude_field_type then -1
                                           else f.order_number
                                      end asc) d
                     where rownum <= v_max_info_cols)
                loop
                    v_field_code := encode_cf(rec.config_field_id);
                    insert into view_opt_map_info_column (
                        field,
                        view_opt_map_id,
                        order_number,
                        program_id
                    ) values (
                        v_field_code,
                        v_view_map_id,
                        rec.order_number,
                        v_sheet_rec.program_id
                    );
                end loop;
            end if;
        end if;

    end create_view;

    procedure update_view(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as

        v_vid view_opt.view_opt_id%type;
        v_view_rec view_opt%rowtype;
        v_new_vid dropgrid_sheet.new_vid%type;
        v_pid dropgrid_sheet.program_id%type;
    begin
        set_status(p_sheet_id, c_tb_view_status);

        select new_vid,
               program_id
          into v_new_vid,
               v_pid
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        if v_new_vid = c_do_not_modify_value_id then
            return;
        end if;

        begin
            select *
              into v_view_rec
              from view_opt
             where view_opt_id = v_new_vid;
        exception
            when no_data_found
                then raise_application_error(-20000, pkg_label.format(17757, pkg_label.list_label_params('view_id' => v_vid)));
        end;

        /*Save view as current*/
        delete from view_opt_current c
         where c.user_id = pkg_sec.get_cu()
           and c.master_ttid is null
           and c.grid_page_id = v_view_rec.grid_page_id
           and exists (select vo.view_opt_id
                         from view_opt vo
                        where vo.view_opt_id = c.view_opt_id
                          and vo.trackor_type_id = v_view_rec.trackor_type_id);

        insert into view_opt_current (
            user_id,
            view_opt_id,
            program_id,
            grid_page_id
        ) values (
            pkg_sec.get_cu(),
            v_view_rec.view_opt_id,
            v_pid,
            v_view_rec.grid_page_id
        );

        delete from view_opt_column
         where view_opt_id = v_view_rec.view_opt_id;

        assign_fields_to_view(p_sheet_id, v_view_rec.view_opt_id, c_tb_module_name);

        update dropgrid_sheet
           set is_view_update = 1,
               tb_view_opt_id = v_view_rec.view_opt_id
         where dropgrid_sheet_id = p_sheet_id;
    end update_view;

    procedure create_tb_err_filter(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) as
        c_module_name constant grid_page.module_name%type := 'TRACKOR_BROWSER';
        v_gpid grid_page.grid_page_id%type;
        v_ttid dropgrid_sheet.dropgrid_sheet_id%type;
        v_pid dropgrid_sheet.program_id%type;
        v_fid filter_opt.filter_opt_id%type;
    begin
        select grid_page_id
          into v_gpid
          from grid_page
         where module_name = c_module_name;

        select trackor_type_id,
               program_id
          into v_ttid,
               v_pid
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

        set_status(p_sheet_id, c_tb_imp_err_filter_status);

        insert into filter_opt (
            grid_page_id,
            name,
            trackor_type_id,
            program_id,
            filter_view_type_id
        ) values (
            v_gpid,
            pkg_label.get_label_system(6192),
            v_ttid,
            v_pid,
            pkg_filter_opt.c_grid_page_filter_type_id
        ) returning filter_opt_id into v_fid;

        update dropgrid_sheet
           set tb_err_filter_opt_id = v_fid
         where dropgrid_sheet_id = p_sheet_id;

        pkg_user.user_obj_exception_modify(
            pkg_sec.get_cu(),
            v_fid,
            c_filter_sec_gr_type_id,
            1);

        insert into filter_opt_attrib (
            filter_opt_id,
            filter_oper_id,
            order_number,
            name,
            program_id
        ) values (
            v_fid,
            c_has_comments_filter_oper_id,
            1,
            's_0_' || c_all_fields_pseudo_cfid,
            v_pid
        );

    end create_tb_err_filter;

    procedure create_report(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_rpt_template_blob_id blob_data.blob_data_id%type,
        p_rpt_sql clob) as

        v_rpt_id report_files.report_file_id%type;
        v_rpt_sheet_id report_worksheet.report_worksheet_id%type;
        v_desc report_files.description%type;
        v_type_lbl label_program.label_program_text%type;
        v_desc_date_str varchar2(16);
        v_rpt_group_id v_report_group.report_group_id%type;
        v_sheet_name dropgrid_sheet.sheet_name%type;
        v_pid dropgrid_sheet.program_id%type;
        v_new_rpt_name dropgrid_sheet.new_report_name%type;
        v_rpt_pattern_id report_pattern_block.report_pattern_block_id%type;
    begin
        select s.new_report_name,
               s.program_id,
               pkg_label.get_label_program(xt.applet_label_id, pkg_sec.get_lang()),
               s.sheet_name,
               to_char(current_date, 'DD/MM/YYYY HH:MI')
          into v_new_rpt_name,
               v_pid,
               v_type_lbl,
               v_sheet_name,
               v_desc_date_str
          from dropgrid_sheet s
          join xitor_type xt on (xt.xitor_type_id = s.trackor_type_id)
         where dropgrid_sheet_id = p_sheet_id;

        set_status(p_sheet_id, c_report_status);

        v_desc := pkg_label.get_label_system(6156, pkg_sec.get_lang());
        v_desc := regexp_replace(v_desc, '{}', v_type_lbl, 1, 1);
        v_desc := regexp_replace(v_desc, '{}', v_desc_date_str, 1, 1);

        begin
            select report_group_id
              into v_rpt_group_id
              from v_report_group
             where report_group = pkg_label.get_label_system(6158, pkg_sec.get_lang())
               and program_id = pkg_sec.get_pid();
        exception
            when no_data_found
                then v_rpt_group_id := null;
        end;

        if (v_rpt_group_id is null) then
            insert into v_report_group (
                report_group,
                program_id
            ) values (
                pkg_label.get_label_system(6158, pkg_sec.get_lang()),
                v_pid
            ) returning report_group_id
                   into v_rpt_group_id;
        end if;

        insert into report_files (
            default_delivery,
            report_delivery,
            default_report_format,
            report_formats,
            report_template_mode_id,
            template_blob_data_id,
            purge_period,
            report_scheduler_id,
            description,
            report_name,
            report_group_id,
            program_id
        ) values (
            c_file_rpt_delivery_id,
            c_file_rpt_delivery_id,
            c_excel_rpt_format_id,
            c_excel_rpt_format_id,
            c_one_rpt_template_mode_id,
            p_rpt_template_blob_id,
            c_def_purge_period,
            c_def_rpt_scheduler_id,
            v_desc,
            v_new_rpt_name,
            v_rpt_group_id,
            v_pid
        ) returning report_file_id
               into v_rpt_id;

        update dropgrid_sheet
           set report_file_id = v_rpt_id
         where dropgrid_sheet_id = p_sheet_id;

        insert into report_worksheet (
            report_file_id,
            worksheet,
            program_id
        ) values (
            v_rpt_id,
            v_sheet_name,
            v_pid
        ) returning report_worksheet_id
               into v_rpt_sheet_id;

        insert into report_pattern_block (
            report_fill_mode_id,
            pattern_block_name,
            first_row,
            last_row,
            report_file_id,
            report_worksheet_id,
            program_id
        ) values (
            c_copy_rpt_fill_mode_id,
            v_sheet_name,
            2,
            2,
            v_rpt_id,
            v_rpt_sheet_id,
            v_pid
        ) returning report_pattern_block_id
               into v_rpt_pattern_id;

        insert into report_sql (
            report_file_id,
            sql_name,
            sql_text,
            sql_type,
            program_id,
            report_pattern_block_id
        ) values (
            v_rpt_id,
            c_rpt_sql_name,
            p_rpt_sql,
            c_data_rpt_sql_type_id,
            v_pid,
            v_rpt_pattern_id
        );

        for rec in (select rownum as ord_num,
                           sql_field_name
                      from (select f.sql_field_name
                              from dropgrid_sheet s
                              join dropgrid_field df on (df.dropgrid_sheet_id = s.dropgrid_sheet_id)
                              join config_field f on (f.config_field_id = df.config_field_id)
                     where s.dropgrid_sheet_id = p_sheet_id
                     order by df.order_number))
        loop
            insert into report_col_mapping(
                pattern_block_col,
                pattern_block_col_idx,
                pattern_block_row,
                program_id,
                report_pattern_block_id,
                sql_col_name
            ) values (
                to_excel_alpha_based(rec.ord_num),
                rec.ord_num,
                1,
                v_pid,
                v_rpt_pattern_id,
                rec.sql_field_name
            );
        end loop;

    end create_report;

    procedure complete_wizard(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_user_err_msg wizard.user_friendly_err_msg%type,
        p_err_msg wizard.err_msg%type) as
    begin
        update wizard set finish_ts = current_date, user_friendly_err_msg = p_user_err_msg, err_msg = p_err_msg
        where wizard_id = (
          select d.wizard_id
          from dropgrid_sheet s
            join dropgrid d on (d.dropgrid_id = s.dropgrid_id)
          where s.dropgrid_sheet_id = p_sheet_id
        );
    end complete_wizard;

    procedure set_status(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type, p_status number) as
        pragma autonomous_transaction;
    begin
        update dropgrid set status_id = p_status
        where dropgrid_id = (select dropgrid_id from dropgrid_sheet where dropgrid_sheet_id = p_sheet_id);
        commit;
    end set_status;

    procedure create_relation_imp_mapping(
        p_imp_id imp_spec.imp_spec_id%type,
        p_imp_entity_id imp_entity.imp_entity_id%type,
        p_parent_ttid xitor_type.xitor_type_id%type,
        p_imp_col_id imp_column.imp_column_id%type) as
        v_imp_data_map_id imp_data_map.imp_data_map_id%type;
        v_name imp_data_map.data_map_name%type;
        v_pid imp_spec.program_id%type;
        v_imp_data_type_id imp_data_type.imp_data_type_id%type;
    begin
        select program_id
          into v_pid
          from imp_spec
         where imp_spec_id = p_imp_id;

        select name
          into v_name
          from imp_column
         where imp_column_id = p_imp_col_id;

        select imp_data_type_id
          into v_imp_data_type_id
          from imp_data_type
         where name = 'Relation';

        insert into imp_data_map (
            data_map_name,
            imp_column_id,
            imp_data_type_id,
            imp_entity_id,
            imp_spec_id,
            order_number,
            program_id
        ) values (
            v_name,
            p_imp_col_id,
            v_imp_data_type_id,
            p_imp_entity_id,
            p_imp_id,
            0,
            v_pid
        ) returning imp_data_map_id into v_imp_data_map_id;

        for rec_param_value in (select imp_data_type_param_id,
                                       case when sql_parameter = c_imp_rel_del_exist_rel then 'No'
                                            when sql_parameter = c_imp_rel_parent_ttid then to_char(p_parent_ttid)
                                            else null
                                       end as value
                                  from imp_data_type_param
                                 where imp_data_type_id = v_imp_data_type_id)
        loop
            insert into imp_data_type_param_value (
                imp_data_map_id,
                imp_data_type_param_id,
                value,
                display_value,
                program_id
            ) values (
                v_imp_data_map_id,
                rec_param_value.imp_data_type_param_id,
                rec_param_value.value,
                rec_param_value.value,
                v_pid
            );
        end loop;
    end create_relation_imp_mapping;

    procedure create_relation_imp_mappings(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_imp_id imp_spec.imp_spec_id%type,
        p_imp_entity_id imp_entity.imp_entity_id%type) as
        v_imp_parent_col_id imp_column.imp_column_id%type;
    begin
        for rec in (
            select (2 * rownum) as ord_num, parent_ttid
            from (
                select new_parent_ttid as parent_ttid
                from (
                    select
                        f.new_parent_ttid,
                        f.new_prim_key,
                        f.order_number,
                        row_number() over (partition by decode(f.new_parent_ttid, null, 1, 0) order by f.order_number) as parent_rn,
                        row_number() over (partition by decode(f.new_parent_ttid, null, decode(f.new_prim_key, null, 0, 1), 1) order by f.order_number) as field_rn,
                        decode(f.new_parent_ttid, null, decode(f.new_prim_key, null, 2, 1), 0) as group_id
                    from dropgrid_field f
                        join config_field cf on (cf.config_field_id = f.config_field_id)
                        left outer join xitor_type xt on (xt.xitor_type_id = f.new_parent_ttid)
                    where f.dropgrid_sheet_id = p_sheet_id
                        and f.new_data_type_id <> c_efile_field_type
                ) where group_id = 0
                order by group_id, decode(new_parent_ttid, null, decode(new_prim_key, null, field_rn, 1), parent_rn)
            )
        ) loop
            select imp_column_id into v_imp_parent_col_id
            from imp_column
            where imp_spec_id = p_imp_id
                and order_number = rec.ord_num;
            create_relation_imp_mapping(
                p_imp_id,
                p_imp_entity_id,
                rec.parent_ttid,
                v_imp_parent_col_id);
        end loop;
    end create_relation_imp_mappings;

    function create_imp_entity(
        p_imp_id imp_spec.imp_spec_id%type,
        p_ttid xitor_type.xitor_type_id%type,
        p_type_lbl_id xitor_type.applet_label_id%type,
        p_imp_col_id imp_column.imp_column_id%type,
        p_ord_num imp_entity.order_number%type)
        return imp_entity.imp_entity_id%type as
        v_entity_id imp_entity.imp_entity_id%type;
        v_is_autokey_type xitor_type.is_autokey%type;
        v_imp_spec_rec imp_spec%rowtype;
        v_entity_name imp_entity.entity_name%type;
    begin
        select * into v_imp_spec_rec
        from imp_spec where imp_spec_id = p_imp_id;

        v_entity_name := substr(pkg_label.get_label_program(p_type_lbl_id, pkg_sec.get_lang()), 1, c_max_imp_entity_name_length);

        insert into imp_entity (
            entity_name
            ,imp_spec_id
            ,order_number
            ,xitor_type_id
            ,sql_text
            ,program_id
        ) values (
            v_entity_name
            ,p_imp_id
            ,p_ord_num
            ,p_ttid
            ,c_imp_entity_sql_part1 || to_char(p_ttid) || c_imp_entity_sql_part2
            ,v_imp_spec_rec.program_id
        ) returning imp_entity_id into v_entity_id;

        if p_imp_col_id is not null then
            insert into imp_entity_param (
                imp_entity_id,
                sql_parameter,
                imp_column_id,
                program_id
            ) values (
                v_entity_id,
                c_imp_entity_key_param_name,
                p_imp_col_id,
                v_imp_spec_rec.program_id
            );
        end if;

        insert into imp_entity_param (
            imp_entity_id
            ,sql_parameter
            ,imp_column_id
            ,parameter_value
            ,program_id
        ) values (
            v_entity_id
            ,c_imp_entity_pid_param_name
            ,null
            ,c_imp_entity_pid_param_val
            ,v_imp_spec_rec.program_id
        );

        if (p_imp_col_id is not null) then
            select is_autokey into v_is_autokey_type
            from xitor_type where xitor_type_id = p_ttid;
            if (v_is_autokey_type = 0) then
                update imp_entity_req_field
                   set imp_column_id = p_imp_col_id
                 where imp_entity_id = v_entity_id
                   and xitor_req_field_id = (select xitor_req_field_id
                                               from xitor_req_field
                                              where field_name = 'TRACKOR_KEY'
                                                and xitor_type_id = p_ttid);
            end if;
        end if;

        update imp_entity_req_field
        set value = 'PROGRAM_ID'
        where imp_entity_id = v_entity_id
            and xitor_req_field_id = (
                select xitor_req_field_id
                from xitor_req_field
                where field_name = 'PROGRAM_ID'
                    and xitor_type_id = p_ttid
            );
        return v_entity_id;
    end create_imp_entity;

    function get_unique_table_name(p_name varchar2) return varchar2 as
        v_name attrib_v_table.attrib_v_table_name%type;
        v_orig_name attrib_v_table.attrib_v_table_name%type;
        v_is_unique boolean := false;
        v_idx number := 1;
        v_table_number number;
    begin
        v_orig_name := p_name;
        v_name := p_name;
        while (not v_is_unique)
        loop
            begin
                select count(attrib_v_table_id) into v_table_number
                from attrib_v_table
                where program_id = pkg_sec.get_pid()
                    and attrib_v_table_name = v_name;
            exception
                when no_data_found then
                    v_table_number := 0;
            end;
            if (v_table_number > 0) then
                v_name := v_orig_name || to_char(v_idx);
                v_idx := v_idx + 1;
            else
                v_is_unique := true;
            end if;
        end loop;
        return v_name;
    end get_unique_table_name;

    function get_prim_field_label(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) return varchar2 as
        v_prim_field_label varchar2(255);
    begin
        begin
            select new_field_label into v_prim_field_label
            from dropgrid_field
            where
                dropgrid_sheet_id = p_sheet_id
                and new_prim_key = 1;
        exception
            when no_data_found then
                v_prim_field_label := null;
        end;
        return v_prim_field_label;
    end get_prim_field_label;

    function get_trackor_class_label(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type) return varchar2
    as
        c_trackor_class_data_type_id dropgrid_field.new_data_type_id%type := -3;

        v_label varchar2(255);
    begin
        begin
            select new_field_label
              into v_label
              from dropgrid_field
             where dropgrid_sheet_id = p_sheet_id
               and new_data_type_id = c_trackor_class_data_type_id;
        exception
            when no_data_found
                then v_label := null;
        end;
        return v_label;
    end get_trackor_class_label;

    function create_trackor_type_int(
                p_name xitor_type.xitor_type%type,
                p_label varchar2,
                p_prefix varchar2,
                p_prim_key_label varchar2,
                p_trackor_class_label varchar2,
                p_pid program.program_id%type) return xitor_type.xitor_type_id%type as
        v_ttid xitor_type.xitor_type_id%type;
        v_sec_group_id sec_group_program.sec_group_program_id%type;
    begin
        insert into xitor_type(
            xitor_type,
            applet_label_id,
            my_xitors_label_id,
            xitor_class_label_id,
            xitorid_label_id,
            prefix_label_id,
            wp_mode_id,
            program_id
        ) values (
            p_name,
            pkg_label.create_label_program(p_label),
            pkg_label.create_label_program(pkg_label.get_label_system(6127, pkg_sec.get_lang()) || p_label),
            pkg_label.create_label_program(nvl(p_trackor_class_label, p_label || pkg_label.get_label_system(6128, pkg_sec.get_lang()))),
            pkg_label.create_label_program(nvl(p_prim_key_label, p_label || pkg_label.get_label_system(6129, pkg_sec.get_lang()))),
            pkg_label.create_label_program(p_prefix || c_trackortype_prefix_delimitor),
            c_no_limits_wp_mode_id,
            p_pid
        ) returning xitor_type_id
               into v_ttid;

        select sec_group_program_id
          into v_sec_group_id
          from sec_group_program
         where security_group = p_name
           and sec_group_type_id = c_trackor_type_sec_gr_type_id
           and program_id = p_pid;
        pkg_sec_priv_program.user_sec_exception_upsert(pkg_sec.get_cu(), v_sec_group_id, 'READ');

        select sec_group_program_id
          into v_sec_group_id
          from sec_group_program
         where security_group = p_name || pkg_const.c_general_tab_postfix
           and sec_group_type_id = c_config_tab_sec_gr_type_id
           and program_id = p_pid;
        pkg_sec_priv_program.user_sec_exception_upsert(pkg_sec.get_cu(), v_sec_group_id, 'RE');

        return v_ttid;
    end create_trackor_type_int;

    procedure assign_fields_to_tab(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_cgid config_group.config_group_id%type) as

        v_sheet_rec dropgrid_sheet%rowtype;
        v_tab_xrefs list_config_element_xref := new list_config_element_xref();
        v_elem_id config_element.config_element_id%type;
        v_row_pos config_group_elem_xref.row_pos%type;
        v_col_pos config_group_elem_xref.col_pos%type := 1;
        v_is_master_tab config_group.is_master_tab%type;
        v_tab_ttid config_group.xitor_type_id%type;
        v_tab_view_id view_opt.view_opt_id%type;
    begin
        select * into v_sheet_rec
        from dropgrid_sheet where dropgrid_sheet_id = p_sheet_id;

        select is_master_tab, xitor_type_id into v_is_master_tab, v_tab_ttid
        from config_group where config_group_id = p_cgid;

        if v_is_master_tab = 1 then
            --Master Tab already has assigned elements
            select nvl(max(row_pos),0) + 1
            into v_row_pos
            from config_group_elem_xref
            where config_group_id = p_cgid;
        else
            v_row_pos := 1;
        end if;

        /*Assign relation fields to the Tab*/
        for rec in (
            select d.*, rownum as ord_num from (
                select cf.*
                from dropgrid_field f
                    join config_field cf on (cf.xitor_type_id = f.new_parent_ttid)
                where f.dropgrid_sheet_id = p_sheet_id
                  and cf.config_field_name = c_trackor_key_name
                  and cf.is_static = 1
                order by f.order_number
            ) d
        ) loop
            v_elem_id := pkg_form.get_or_create_field_element(p_cgid, rec.config_field_id);
            v_tab_xrefs.extend();
            v_tab_xrefs(v_tab_xrefs.count) := new t_config_element_xref(
               p_cgid,
               v_elem_id,
               v_row_pos,
               v_col_pos
            );
            v_col_pos := v_col_pos + 1;
            if (v_col_pos > 2) then
                v_row_pos := v_row_pos + 1;
                v_col_pos := 1;
            end if;
        end loop;

        if (v_tab_xrefs.count > 0) then
            /*Create horizonal splitter between relation and config fields*/
            v_elem_id := pkg_form.create_splitter_element(v_sheet_rec.program_id);
            v_row_pos := v_row_pos + 1;
            v_col_pos := 1;
            v_tab_xrefs.extend();
            v_tab_xrefs(v_tab_xrefs.count) := new t_config_element_xref(
               p_cgid,
               v_elem_id,
               v_row_pos,
               v_col_pos
            );
            v_row_pos := v_row_pos + 1;
        end if;

        for rec in (
            select f.*
            from dropgrid_field f
            where f.dropgrid_sheet_id = v_sheet_rec.dropgrid_sheet_id
                and f.config_field_id is not null and nvl(new_parent_ttid,0) = 0
            order by f.order_number
        ) loop
            v_elem_id := pkg_form.get_or_create_field_element(p_cgid, rec.config_field_id);
            v_tab_xrefs.extend();
            v_tab_xrefs(v_tab_xrefs.count) := new t_config_element_xref(
               p_cgid,
               v_elem_id,
               v_row_pos,
               v_col_pos
            );
            v_col_pos := v_col_pos + 1;
            if (v_col_pos > 2) then
                v_row_pos := v_row_pos + 1;
                v_col_pos := 1;
            end if;
        end loop;

        pkg_form.assign_elements_to_tab(p_cgid, v_tab_xrefs);

        if v_is_master_tab = 1 then
            v_tab_view_id := pkg_view_opt.update_master_tab_view(v_tab_ttid, v_sheet_rec.program_id);
        end if;
    end assign_fields_to_tab;

    function encode_cf(p_cfid config_field.config_field_id%type)
        return varchar2 as
        v_cf_rec config_field%rowtype;
        v_code varchar2(100);
    begin
        select * into v_cf_rec
        from config_field where config_field_id = p_cfid;

        if (v_cf_rec.config_field_name = c_trackor_key_name) then
            v_code := 's_' || v_cf_rec.xitor_type_id || '_0';
        elsif (v_cf_rec.config_field_name = c_trackor_class_name) then
            v_code := 's_' || v_cf_rec.xitor_type_id || '_4';
        else
            v_code := 'd_' || v_cf_rec.xitor_type_id || '_' || v_cf_rec.config_field_id;
        end if;
        return v_code;
    end encode_cf;

    procedure assign_fields_to_view(
        p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type,
        p_vid view_opt.view_opt_id%type,
        p_module_name grid_page.module_name%type) as

        v_max_cols number;
        v_field_code varchar2(50);
        v_pid program.program_id%type;
    begin
        select program_id into v_pid from dropgrid_sheet where dropgrid_sheet_id = p_sheet_id;
        if (p_module_name = c_map_module_name) then
            v_max_cols := to_number(pkg_const.get_param_program_val('MapperMaxLeftSideCols', v_pid));
        else
            v_max_cols := to_number(pkg_const.get_param_program_val('TrackorBrowserMaxCols', v_pid));
        end if;

        for rec in (
            select d.*, rownum as ord_num from (
                select f.*
                from dropgrid_field f
                where f.dropgrid_sheet_id = p_sheet_id
                    and f.config_field_id is not null
                order by f.order_number
            ) d where rownum <= v_max_cols
        ) loop
            v_field_code := encode_cf(rec.config_field_id);
            insert into view_opt_column (
                grid_column
                ,order_number
                ,view_opt_id
                ,program_id
            ) values (
                v_field_code
                ,rec.ord_num
                ,p_vid
                ,rec.program_id
            );
        end loop;
    end assign_fields_to_view;

    function clone_dropgrid(
        p_dgid dropgrid.dropgrid_id%type)
        return dropgrid.dropgrid_id%type as

        v_new_dgid dropgrid.dropgrid_id%type;
        v_blob_id dropgrid.blob_data_id%type;
        v_wid wizard.wizard_id%type;
        v_pid program.program_id%type;
    begin
        select blob_data_id,program_id into v_blob_id,v_pid
        from dropgrid where dropgrid_id = p_dgid;

        insert into wizard (
            wizard_type_id,
            wizard_step_id,
            program_id
        ) values (
            c_dropgrid_wizard_type_id,
            c_2nd_wizard_step_id,
            v_pid
        ) returning wizard_id into v_wid;

        insert into dropgrid(
            blob_data_id
            ,is_config_parsed
            ,wizard_id
            ,program_id
        ) values (
            v_blob_id
            ,0
            ,v_wid
            ,v_pid
        ) returning dropgrid_id into v_new_dgid;
        return v_new_dgid;


    end clone_dropgrid;

    function to_excel_alpha_based(p_col_num pls_integer) return varchar2 as
        v_dividend pls_integer;
        v_col_name varchar2(5) := '';
        v_modulo pls_integer;
    begin
        v_dividend := p_col_num;
        while v_dividend > 0 loop
            v_modulo := mod((v_dividend - 1), 26);
            v_col_name := chr((mod(v_modulo, 26)) + 65) || v_col_name;
            v_dividend := (v_dividend - v_modulo) / 26;
        end loop;
        return v_col_name;
    end to_excel_alpha_based;

    function remove_invalid_chars_in_name(p_name varchar2) return varchar2 as
      v_name varchar2(300);
    begin
        --Drop unavailable characters like ! ? & ( ) * + , - . / : < = > @ [ ] { } ' ; | space.
        --You can use only alphabetic, numeric and underscore characters
        v_name := regexp_replace(p_name, '[^_[:alnum:]]+', '');

        --Drop non alphabetic characters at the begining of string
        v_name := substr(v_name, regexp_instr(v_name, '[[:alpha:]]', 1, 1));
        return v_name;
    end remove_invalid_chars_in_name;

    function get_unique_trackor_type_name(
      p_name xitor_type.xitor_type%type,
      p_pid program.program_id%type) return xitor_type.xitor_type%type as
      v_name xitor_type.xitor_type%type;
    begin
      select target_name into v_name
      from (
      select root || (level+1) as target_name, connect_by_isleaf as is_leaf
      from
        (select p_name as root, xitor_type from xitor_type where program_id = p_pid) xt
      start with xt.xitor_type = p_name
      connect by (xt.root || level) = xt.xitor_type
      ) where is_leaf = 1;

      if v_name is null then
        v_name := p_name;
      end if;
      return v_name;
    exception
      when no_data_found then
        return p_name;
    end get_unique_trackor_type_name;

    procedure assign_trackor_classes(p_sheet_id dropgrid_sheet.dropgrid_sheet_id%type)
    as
        v_is_trackor_update dropgrid_sheet.is_trackor_update%type;
        v_ttid dropgrid_sheet.trackor_type_id%type;
        v_new_cgid dropgrid_sheet.new_cgid%type;
        v_tab_id dropgrid_sheet.config_group_id%type;
    begin
        select is_trackor_update,
               trackor_type_id,
               new_cgid,
               config_group_id
          into v_is_trackor_update,
               v_ttid,
               v_new_cgid,
               v_tab_id
          from dropgrid_sheet
         where dropgrid_sheet_id = p_sheet_id;

         if v_is_trackor_update = 0 and v_new_cgid = 0 then
            insert into config_group_xitor_class_xref(xitor_class_id,config_group_id,program_id)
            select xitor_class_id,
                   v_tab_id,
                   program_id
              from v_xitor_class
             where xitor_type_id = v_ttid;
         end if;
    end assign_trackor_classes;
end pkg_wiz_dropgrid;
/