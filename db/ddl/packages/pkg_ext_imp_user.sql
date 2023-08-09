CREATE OR REPLACE PACKAGE BODY PKG_EXT_IMP_USER 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
    cursor cur_user_load_line(p_rid in imp_run.imp_run_id%type)
    is
    select row_num,
           pkg_imp_utils.get_col_data(p_rid, 'UN', irg.row_num) as un,
           pkg_imp_utils.get_col_data(p_rid, 'EMAIL', irg.row_num) as email,
           pkg_imp_utils.get_col_data(p_rid, 'IS_DISABLED', irg.row_num) as is_disabled,
           pkg_imp_utils.get_col_data(p_rid, 'MUST_CHANGE_PASSWORD', irg.row_num) as must_change_password,
           pkg_imp_utils.get_col_data(p_rid, 'INIT_PASSWORD_RESET', irg.row_num) as init_password_reset,
           pkg_imp_utils.get_col_data(p_rid, 'IS_SUPERUSER', irg.row_num) as is_superuser,
           pkg_imp_utils.get_col_data(p_rid, 'Menu', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'APPLICATION', irg.row_num) as menu,
           pkg_imp_utils.get_col_data(p_rid, 'Security Role 1', irg.row_num) as secrole1,
           pkg_imp_utils.get_col_data(p_rid, 'Security Role 2', irg.row_num) as secrole2,
           pkg_imp_utils.get_col_data(p_rid, 'Security Role 3', irg.row_num) as secrole3,
           pkg_imp_utils.get_col_data(p_rid, 'SECURITY_ROLES', irg.row_num) as sec_roles,
           pkg_imp_utils.get_col_data(p_rid, 'Global Views', irg.row_num) as globalviews,
           pkg_imp_utils.get_col_data(p_rid, 'Global Filters', irg.row_num) as filters,
           pkg_imp_utils.get_col_data(p_rid, 'Discipline 1', irg.row_num) as discip1,
           pkg_imp_utils.get_col_data(p_rid, 'Discipline 2', irg.row_num) as discip2,
           pkg_imp_utils.get_col_data(p_rid, 'Discipline 3', irg.row_num) as discip3,
           pkg_imp_utils.get_col_data(p_rid, 'IS_ADD_QUOTE_DELIM', irg.row_num) as is_add_quote_delim,
           pkg_imp_utils.get_col_data(p_rid, 'IS_AUTO_SAVE_TB_GRID_CHANGES', irg.row_num) as is_auto_save_tb_grid_changes,
           pkg_imp_utils.get_col_data(p_rid, 'CHECKBOX_MODE_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'CHECKBOX_MODE', irg.row_num) as checkbox_mode,
           pkg_imp_utils.get_col_data(p_rid, 'COORDINATE_MODE_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'COORDINATE_MODE', irg.row_num) as coordinate_mode,
           pkg_imp_utils.get_col_data(p_rid, 'DATE_FORMAT', irg.row_num) as date_format,
           pkg_imp_utils.get_col_data(p_rid, 'DEFAULT_PAGE_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'DEFAULT_PAGE', irg.row_num) as default_page,
           pkg_imp_utils.get_col_data(p_rid, 'IS_EXACT_SEARCH_CLIPBOARD', irg.row_num) as is_exact_search_clipboard,
           pkg_imp_utils.get_col_data(p_rid, 'FV_LIST_MODE_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'FV_LIST_MODE', irg.row_num) as fv_list_mode,
           pkg_imp_utils.get_col_data(p_rid, 'IS_COMMENTS_ON_MOUSE_OVER', irg.row_num) as is_comments_on_mouse_over,
           pkg_imp_utils.get_col_data(p_rid, 'GRID_EDIT_MODE_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'GRID_EDIT_MODE', irg.row_num) as grid_edit_mode,
           pkg_imp_utils.get_col_data(p_rid, 'IS_HIDE_START_DATE', irg.row_num) as is_hide_start_date,
           pkg_imp_utils.get_col_data(p_rid, 'IS_MUTE_NEW_EVENTS', irg.row_num) as is_mute_new_events,
           pkg_imp_utils.get_col_data(p_rid, 'APP_LANG_ID', irg.row_num) || pkg_imp_utils.get_col_data(p_rid, 'APP_LANG_NAME', irg.row_num) as app_lang_name,
           pkg_imp_utils.get_col_data(p_rid, 'IS_SHOW_TIP_OF_THE_DAY', irg.row_num) as is_show_tip_of_the_day,
           pkg_imp_utils.get_col_data(p_rid, 'THOUSANDS_SEPARATOR', irg.row_num) as thousands_separator,
           pkg_imp_utils.get_col_data(p_rid, 'TIME_FORMAT', irg.row_num) as time_format,
           pkg_imp_utils.get_col_data(p_rid, 'OWNER_XITOR_TYPE', irg.row_num) as owner_xitor_type,
           pkg_imp_utils.get_col_data(p_rid, 'IS_EXTERNAL_LOGIN', irg.row_num) as is_external_login,
           pkg_imp_utils.get_col_data(p_rid, 'MFA_TYPE', irg.row_num) as mfa_type,
           pkg_imp_utils.get_col_data(p_rid, 'MAXIMIZE_NEW_WINDOW', irg.row_num) as maximize_new_window,
           pkg_imp_utils.get_col_data(p_rid, 'LINKED_VALUES_DISP_MODE', irg.row_num) as linked_values_disp_mode,
           pkg_imp_utils.get_col_data(p_rid, 'IS_CASE_SENSITIVE_SORTING', irg.row_num) as is_case_sensitive_sorting,
           pkg_imp_utils.get_col_data(p_rid, 'IS_HIDE_FIELD_TAB_PREFIX', irg.row_num) as is_hide_field_tab_prefix,
           pkg_imp_utils.get_col_data(p_rid, 'IS_MAIN_MENU_STICKY', irg.row_num) as is_main_menu_sticky,
           pkg_imp_utils.get_col_data(p_rid, 'PHONE_NUMBER', irg.row_num) as phone_number,
           pkg_imp_utils.get_col_data(p_rid, 'IS_SHOW_USER_CHAT_STATE_IN_GRID', irg.row_num) as is_show_user_chat_state_in_grid
      from imp_run_grid irg
     where imp_run_id = p_rid
       and col_num = 1
       and row_num > (select start_row
                        from imp_run
                       where imp_run_id = p_rid);

    cursor cur_cf_data(p_rid in imp_run.imp_run_id%type, p_row_num in number)
    is
    select data, col_num
      from imp_run_grid
     where data is not null
       and imp_run_id = p_rid
       and row_num = p_row_num
       and col_num in (select col_num
                         from imp_run_grid
                        where imp_run_id = p_rid
                          and row_num = (select start_row
                                           from imp_run
                                          where imp_run_id = p_rid)
                          and data not in (
                             'USER_ID', 'UN', 'EMAIL', 'IS_DISABLED', 'MUST_CHANGE_PASSWORD', 'INIT_PASSWORD_RESET',
                             'IS_SUPERUSER', 'Menu', 'Security Role 1', 'Security Role 2', 'Security Role 3',
                             'Global Views', 'Global Filters', 'Discipline 1', 'Discipline 2', 'Discipline 3',
                             'IS_ADD_QUOTE_DELIM', 'IS_AUTO_SAVE_TB_GRID_CHANGES', 'CHECKBOX_MODE_ID',
                             'COORDINATE_MODE_ID', 'DATE_FORMAT', 'DEFAULT_PAGE_ID', 'IS_EXACT_SEARCH_CLIPBOARD',
                             'FV_LIST_MODE_ID', 'IS_COMMENTS_ON_MOUSE_OVER',
                             'GRID_EDIT_MODE_ID', 'IS_HIDE_START_DATE',
                             'IS_MUTE_NEW_EVENTS', 'APP_LANG_ID',
                             'IS_SHOW_TIP_OF_THE_DAY', 'THOUSANDS_SEPARATOR', 'TIME_FORMAT', 'OWNER_XITOR_TYPE', 'XITOR_KEY', 'LINE_NUM', 'IS_EXTERNAL_LOGIN',
                             'APPLICATION', 'CHECKBOX_MODE', 'CLICK_ON_PHOTO_MODE', 'COORDINATE_MODE', 'DEFAULT_PAGE', 'FV_LIST_MODE', 'GRID_EDIT_MODE', 'APP_LANG_NAME',
                             'MFA_TYPE', 'MAXIMIZE_NEW_WINDOW', 'LINKED_VALUES_DISP_MODE', 'IS_SHOW_USER_CHAT_STATE_IN_GRID',
                             'IS_CASE_SENSITIVE_SORTING', 'MODIFIED_BY', 'MODIFIED_AT', 'CREATED_BY', 'CREATED_AT',
                             'IS_HIDE_FIELD_TAB_PREFIX', 'IS_MAIN_MENU_STICKY', 'SECURITY_ROLES', 'PHONE_NUMBER'));

    function search_valid_user_ttid(p_rid in imp_run.imp_run_id%type,
        p_user_ttid in xitor_type.xitor_type_id%type)
    return xitor_type.xitor_type_id%type;

    procedure import_user(line in out nocopy cur_user_load_line%rowtype,
        p_user_ttid in xitor_type.xitor_type_id%type,
        p_rid in imp_run.imp_run_id%type,
        p_update_xitors in number,
        p_xkey_val xitor.xitor_key%type);

    /**
      * Assign application (menu) to user, log error if application not found;
      */
    procedure assign_menu(p_app_name application.name%type,
        p_uid in users.user_id%type, p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type);

    procedure assign_global_views(p_assign_views in varchar2,
        p_uid in users.user_id%type);

    procedure assign_global_filters(p_assign_filters in varchar2,
        p_uid in users.user_id%type);

    /**
      * Assign discipline to user, log error if discipline not found;
      */
    procedure assign_discp(p_discp v_discp.discp_type%type,
        p_uid in users.user_id%type, p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type);

    /**
      * Search xitor for specifed user, or create new if it doen't exists.
      *
      * @return xitor_id
      */
    function get_user_xitor_pk(p_user_ttid xitor_type.xitor_type_id%type,
        p_uid in users.user_id%type,
        p_is_new in out boolean,
        p_xkey_val in xitor.xitor_key%type)
    return xitor.xitor_id%type;

    /**
      * Import row of config fields data. Also can import static fields of xitor table.
      *
      * @param p_row_num number of row with data
      * @param p_xt_id xitor type id of field to import
      * @param p_rid imp_run_id
      */
    procedure imp_cfields_data(
        p_xid in xitor.xitor_id%type,
        p_row_num in number,
        p_xt_id in xitor_type.xitor_type_id%type,
        p_rid in imp_run.imp_run_id%type);

    procedure get_parent_type_info(p_child_type_id in number,
        p_col_name in varchar2,
        p_parent_type_id out number,
        p_parent_type_col out varchar2,
        p_relation_type_id out number);

    procedure UsersLoad(p_rid in imp_run.imp_run_id%type,
        p_update_xitors in number default 0,
        p_user_ttid in xitor_type.xitor_type_id%type default null,
        p_key_col_name in varchar2 default null)
    as
        rowcount number := 0;
        v_user_ttid xitor_type.xitor_type_id%type;
        v_xkey_col_num imp_run_grid.col_num%type;
        v_xkey_val xitor.xitor_key%type;
    begin
        if (p_update_xitors = 1) then
            v_user_ttid := search_valid_user_ttid(p_rid, p_user_ttid);
            if (v_user_ttid is null) then
                return;
            end if;
        end if;

        if (p_key_col_name is not null) then
            v_xkey_col_num := pkg_imp_utils.get_col_num(p_rid, p_key_col_name);
        end if;

        for line in cur_user_load_line(p_rid)
        loop
            if (p_key_col_name is not null) then
                v_xkey_val := pkg_imp_run.cell_value(p_rid, line.row_num, v_xkey_col_num);
            end if;

            import_user(line, v_user_ttid, p_rid, p_update_xitors, v_xkey_val);

            rowcount := rowcount + 1;
        end loop;
        pkg_imp_run.set_rows(p_rid, rowcount);
    end UsersLoad;

    function search_valid_user_ttid(p_rid in imp_run.imp_run_id%type,
        p_user_ttid in xitor_type.xitor_type_id%type)
    return xitor_type.xitor_type_id%type
    is
        v_user_ttid xitor_type.xitor_type_id%type;
        v_err_msg clob;
        v_pid number;
    begin
        select program_id into v_pid from imp_run where imp_run_id = p_rid;

        if (p_user_ttid is not null) then
            begin
                select xitor_type_id into v_user_ttid
                from xitor_type
                where xitor_type_id = p_user_ttid
                      and is_user = 1
                      and program_id = v_pid;
            exception
                when no_data_found then
                    v_err_msg := 'Specified trackor type id: "' || p_user_ttid || '" not "User" trackor type. Need to update External Procedure on Administer Import page';
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
                    return null;
            end;
        else
            begin
                select xitor_type_id into v_user_ttid
                from xitor_type
                where is_user = 1
                      and program_id = v_pid;
            exception
                when no_data_found then
                    v_err_msg := 'Can''t find "User" trackor type. Need to update External Procedure on Administer Import page';
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
                    return null;
                when too_many_rows then
                    v_err_msg := 'Find many "User" trackor type. Need to update External Procedure on Administer Import page';
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
                    return null;
            end;
        end if;

        return v_user_ttid;
    end search_valid_user_ttid;

    procedure import_user(
        line in out nocopy cur_user_load_line%rowtype,
        p_user_ttid in xitor_type.xitor_type_id%type,
        p_rid in imp_run.imp_run_id%type,
        p_update_xitors in number,
        p_xkey_val xitor.xitor_key%type)
    is
    pragma autonomous_transaction;
        v_retval varchar2(4000);
        v_err_msg clob;
        v_user_id users.user_id%type;
        v_user_xid users.xitor_id%type;
        v_is_new boolean;
        v_err_type number(2);
        v_rt varchar2(20);
        v_new_user boolean := false;
        v_ret_str varchar2(1000);
        v_pid number;
        v_app_lang_id app_languages.app_lang_id%type;
        v_user_tt_id xitor_type.xitor_type_id%type;
        v_menu_item_id users.default_menu_item_id%type;
        v_mfa_type_id users.mfa_type_id%type;
        v_linked_values_disp_mode_id users.linked_values_disp_mode_id%type;
        v_phone_number t_phone_number;
        v_secret_key auth_token.secret_key%type;
        v_secret_key_encrypt auth_token.secret_key%type;
    begin
        select program_id into v_pid from imp_run where imp_run_id = p_rid;

        begin
            select user_id, xitor_id, app_lang_id into v_user_id, v_user_xid, v_app_lang_id
              from users
             where un = trim(line.un)
               and program_id = v_pid;
        exception
            when no_data_found then
                insert into users
                       (un,
                        pwd,
                        email,
                        is_disabled,
                        must_change_password,
                        program_id,
                        app_lang_id,
                        fv_list_mode_id,
                        grid_edit_mode_id,
                        is_comments_on_mouse_over,
                        is_mute_new_events,
                        is_show_tip_of_the_day,
                        maximize_new_window,
                        thousands_separator,
                        is_hide_start_date,
                        is_exact_search_clipboard,
                        is_add_quote_delim,
                        is_auto_save_tb_grid_changes,
                        date_format,
                        time_format,
                        checkbox_mode_id,
                        coordinate_mode_id,
                        mfa_type_id,
                        linked_values_disp_mode_id,
                        is_case_sensitive_sorting,
                        is_hide_field_tab_prefix,
                        is_main_menu_sticky,
                        is_show_user_chat_state_in_grid)
                values (trim(line.un),
                        pkg_vqsecurity.encrypt(dbms_random.string('x', c_password_min_length)),
                        trim(line.email),
                        0,
                        1,
                        v_pid,
                        pkg_const.get_param_program_val('UserDefaultLanguage', v_pid),
                        pkg_const.get_param_program_val('UserDefaultFVListMode', v_pid),
                        pkg_const.get_param_program_val('UserDefaultGridEditMode', v_pid),
                        pkg_const.get_param_program_val('UserDefaultCommentsOnMouseOver', v_pid),
                        pkg_const.get_param_program_val('UserDefaultMuteNewEvents', v_pid),
                        pkg_const.get_param_program_val('UserDefaultShowTipOfTheDay', v_pid),
                        pkg_const.get_param_program_val('UserDefaultMaximizeNewWindow', v_pid),
                        pkg_const.get_param_program_val('UserDefaultThousandsSeparator', v_pid),
                        pkg_const.get_param_program_val('UserDefaultHideStartDate', v_pid),
                        pkg_const.get_param_program_val('UserDefaultExactSearchClipboard', v_pid),
                        pkg_const.get_param_program_val('UserDefaultAddQuoteDelim', v_pid),
                        pkg_const.get_param_program_val('UserDefaultAutoSaveTbGridChanges', v_pid),
                        pkg_const.get_param_program_val('UserDefaultDateFormat', v_pid),
                        pkg_const.get_param_program_val('UserDefaultTimeFormat', v_pid),
                        pkg_const.get_param_program_val('UserDefaultCheckBoxMode', v_pid),
                        pkg_const.get_param_program_val('UserDefaultCoordinateMode', v_pid),
                        pkg_const.get_param_program_val('UserDefaultMfaType', v_pid),
                        pkg_const.get_param_program_val('UserDefaultLinkedValuesDispModeId', v_pid),
                        pkg_const.get_param_program_val('UserDefaultCaseSensitiveSorting', v_pid),
                        pkg_const.get_param_program_val('UserDefaultHideFieldTabPrefix', v_pid),
                        pkg_const.get_param_program_val('UserDefaultMainMenuSticky', v_pid),
                        pkg_const.get_param_program_val('UserDefaultShowUserChatStateInGrid', v_pid))
              returning user_id into v_user_id;

            v_new_user := true;
        end;

        --app_lang_name
        if trim(line.app_lang_name) is not null then
            begin
                select app_lang_id into v_app_lang_id
                  from app_languages
                 where app_lang_description = trim(line.app_lang_name);

                update users set app_lang_id = v_app_lang_id
                 where user_id = v_user_id;
            exception
                when others then
                    v_app_lang_id := null;
            end;
        end if;

        --UN
        -- must be here, because next block use it
        if trim(line.un) is not null then
            update users set un = trim(line.un)
             where user_id = v_user_id;
        end if;

        --owner_xitor_type
        if v_user_xid is null and trim(line.owner_xitor_type) is not null and v_app_lang_id is not null then
            select xt.xitor_type_id into v_user_tt_id
              from xitor_type xt join app_languages al on al.app_lang_id = v_app_lang_id
                                 join vw_label l on l.label_id = xt.applet_label_id and l.app_lang_id = v_app_lang_id
             where l.label_text = trim(line.owner_xitor_type)
               and xt.is_user = 1
               and program_id = pkg_sec.get_pid;

            --create xitor and set xitor id
            v_user_xid := get_user_xitor_pk(v_user_tt_id, v_user_id, v_is_new, p_xkey_val);
        end if;

        -- Email
        if (not pkg_imp_utils.is_empty_str(trim(line.email))) then
            update users set email = trim(line.email) where user_id = v_user_id;
        end if;

        -- Is Disabled
        if (not pkg_imp_utils.is_empty_str(line.is_disabled)) then
            update users set is_disabled = pkg_imp_utils.convert_boolean(line.is_disabled)
             where user_id = v_user_id;
        end if;

        -- Must Change Password
        if (not pkg_imp_utils.is_empty_str(line.must_change_password)) then
            update users set must_change_password = pkg_imp_utils.convert_boolean(line.must_change_password)
             where user_id = v_user_id;
        end if;

        -- Is Superuser
        if (not pkg_imp_utils.is_empty_str(line.is_superuser)) then
            update users set is_superuser = pkg_imp_utils.convert_boolean(line.is_superuser)
             where user_id = v_user_id;
        end if;

        -- is_add_quote_delim
        if trim(line.is_add_quote_delim) is not null then
            update users set is_add_quote_delim = pkg_imp_utils.convert_boolean(trim(line.is_add_quote_delim))
             where user_id = v_user_id;
        end if;

        -- is_auto_save_tb_grid_changes
        if trim(line.is_auto_save_tb_grid_changes) is not null then
            update users set is_auto_save_tb_grid_changes = pkg_imp_utils.convert_boolean(trim(line.is_auto_save_tb_grid_changes))
             where user_id = v_user_id;
        end if;

        -- date_format
        if trim(line.date_format) is not null
            and trim(line.date_format) in ('DD.MM.YY', 'DD.MM.YYYY', 'DD/MM/YY', 'DD/MM/YYYY', 'MM.DD.YY', 'MM.DD.YYYY', 'MM/DD/YY', 'MM/DD/YYYY', 'DD-MON-YYYY', 'YYYY-MM-DD') then
                update users set date_format = trim(line.date_format)
                 where user_id = v_user_id;
        end if;

        -- default_page
        if (not pkg_imp_utils.is_empty_str(line.menu)) then
            begin
                if trim(line.default_page) = pkg_label.get_label_system(3926, v_app_lang_id) or trim(line.default_page) is null then
                    update users
                       set default_menu_item_id = null,
                           default_favorite_page_id = null
                     where user_id = v_user_id;
                else
                    select menu_item_id into v_menu_item_id
                      from (select mi.menu_item_id,
                                   (select label_program_text
                                      from label_program lp
                                     where lp.label_program_id = mi.label_id
                                       and app_lang_id in (select app_lang_id
                                                             from users
                                                            where user_id = v_user_id)) as page_program_label,
                                   (select label_system_text
                                      from label_system ls
                                     where ls.label_system_id = mi.label_id
                                       and app_lang_id in (select app_lang_id
                                                             from users
                                                            where user_id = v_user_id)) as page_system_label
                              from menu_items_app_xref miax, menu_items mi
                             where miax.application_id = (select application_id
                                                            from application
                                                           where upper(name) = upper(line.menu)
                                                             and program_id in (v_pid, 0))
                               and mi.menu_item_id = miax.menu_item_id)
                      where nvl(page_program_label, page_system_label) = trim(line.default_page);

                    update users set default_menu_item_id = v_menu_item_id, default_favorite_page_id = null
                     where user_id = v_user_id;

                 end if;
            exception
                when others then
                    v_menu_item_id := null;
            end;
        end if;

        -- is_exact_search_clipboard
        if trim(line.is_exact_search_clipboard) is not null then
            update users set is_exact_search_clipboard = pkg_imp_utils.convert_boolean(trim(line.is_exact_search_clipboard))
             where user_id = v_user_id;
        end if;

        --is_comments_on_mouse_over
        if trim(line.is_comments_on_mouse_over) is not null then
            update users set is_comments_on_mouse_over = pkg_imp_utils.convert_boolean(trim(line.is_comments_on_mouse_over))
             where user_id = v_user_id;
        end if;

        -- is_show_user_chat_state_in_grid
        if trim(line.is_show_user_chat_state_in_grid) is not null then
            update users set is_show_user_chat_state_in_grid = pkg_imp_utils.convert_boolean(trim(line.is_show_user_chat_state_in_grid))
            where user_id = v_user_id;
        end if;

        --grid_edit_mode
        if trim(line.grid_edit_mode) is not null
            and trim(line.grid_edit_mode) in(pkg_label.get_label_system(3600, v_app_lang_id), pkg_label.get_label_system(3601, v_app_lang_id)) then

                update users set grid_edit_mode_id = case when trim(line.grid_edit_mode) = pkg_label.get_label_system(3600, v_app_lang_id)
                                                          then 0
                                                          else 1 end
                 where user_id = v_user_id;
        end if;

        --coordinate_mode
        if trim(line.coordinate_mode) is not null
            and trim(line.coordinate_mode) in (pkg_label.get_label_system(3936, v_app_lang_id), pkg_label.get_label_system(3937, v_app_lang_id)) then

                update users set coordinate_mode_id = case when trim(line.coordinate_mode) = pkg_label.get_label_system(3936, v_app_lang_id)
                                                        then 0
                                                        else 1 end
                 where user_id = v_user_id;
        end if;

        --fv_list_mode
        if trim(line.fv_list_mode) is not null
            and trim(line.fv_list_mode) in(pkg_label.get_label_system(4003, v_app_lang_id), pkg_label.get_label_system(4004, v_app_lang_id)) then

                update users set fv_list_mode_id = case when trim(line.fv_list_mode) = pkg_label.get_label_system(4003, v_app_lang_id)
                                                     then 0
                                                     else 1
                                                end
                 where user_id = v_user_id;
        end if;

        -- checkbox_mode
        -- 0 - Yes/No Drop-down, 1-Checkbox
        if trim(line.checkbox_mode) is not null
            and trim(line.checkbox_mode) in(pkg_label.get_label_system(3980, v_app_lang_id), pkg_label.get_label_system(3981, v_app_lang_id)) then

                update users set checkbox_mode_id = case when trim(line.checkbox_mode) = pkg_label.get_label_system(3980, v_app_lang_id)
                                                      then 0
                                                      else 1
                                                 end
                 where user_id = v_user_id;
        end if;

        --is_hide_start_date
        if trim(line.is_hide_start_date) is not null then
            update users set is_hide_start_date = pkg_imp_utils.convert_boolean(trim(line.is_hide_start_date))
             where user_id = v_user_id;
        end if;

        --is_mute_new_events
        if trim(line.is_mute_new_events) is not null then
            update users set is_mute_new_events = pkg_imp_utils.convert_boolean(trim(line.is_mute_new_events))
             where user_id = v_user_id;
        end if;

        --is_show_tip_of_the_day
        if trim(line.is_show_tip_of_the_day) is not null then
            update users set is_show_tip_of_the_day = pkg_imp_utils.convert_boolean(trim(line.is_show_tip_of_the_day))
             where user_id = v_user_id;
        end if;

        --is_external_login
        if trim(line.is_external_login) is not null then
            update users set is_external_login = pkg_imp_utils.convert_boolean(trim(line.is_external_login))
             where user_id = v_user_id;
        end if;

        --thousands_separator
        if line.thousands_separator in ('Comma', 'Space', ',', ' ') then
            update users set thousands_separator = case when line.thousands_separator in ('Comma', ' ') then ' '
                                                        else ','
                                                    end
             where user_id = v_user_id;
        end if;

        --time_format
        if trim(line.time_format) in ('HH:mm:ss', 'hh:mm:ss aa') then
            update users set time_format = trim(line.time_format)
             where user_id = v_user_id;
        end if;

        --mfa_type
        if trim(line.mfa_type) is not null then
            begin
                select mfa_type_id into v_mfa_type_id
                  from mfa_type
                 where upper(name) = upper(line.mfa_type);

                update users set mfa_type_id = v_mfa_type_id
                 where user_id = v_user_id;
            exception
                when no_data_found then
                    v_err_msg := 'MFA Type [' || line.mfa_type || '] not found';
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            end;
        end if;

        --maximize_new_window
        if trim(line.maximize_new_window) is not null then
            update users set maximize_new_window = pkg_imp_utils.convert_boolean(trim(line.maximize_new_window))
             where user_id = v_user_id;
        end if;

        -- linked_values_disp_mode
        if trim(line.linked_values_disp_mode) is not null then
            begin
                select linked_values_disp_mode_id
                  into v_linked_values_disp_mode_id
                  from v_linked_values_disp_mode
                 where upper(name) = upper(line.linked_values_disp_mode);

                update users set linked_values_disp_mode_id = v_linked_values_disp_mode_id
                 where user_id = v_user_id;
            exception
                when no_data_found then
                    v_err_msg := 'Linked Values Display Mode [' || line.linked_values_disp_mode || '] not found';
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            end;
        end if;

        --is_case_sensitive_sorting
        if trim(line.is_case_sensitive_sorting) is not null then
            update users
               set is_case_sensitive_sorting = pkg_imp_utils.convert_boolean(trim(line.is_case_sensitive_sorting))
             where user_id = v_user_id;
        end if;

        --is_hide_field_tab_prefix
        if trim(line.is_hide_field_tab_prefix) is not null then
            update users
               set is_hide_field_tab_prefix = pkg_imp_utils.convert_boolean(trim(line.is_hide_field_tab_prefix))
             where user_id = v_user_id;
        end if;

        --is_main_menu_sticky
        if trim(line.is_main_menu_sticky) is not null then
            update users
               set is_main_menu_sticky = pkg_imp_utils.convert_boolean(trim(line.is_main_menu_sticky))
             where user_id = v_user_id;
        end if;

        -- phone number
        if trim(line.phone_number) is not null then
            begin
                v_phone_number := pkg_vqutils.parse_phone_number(trim(line.phone_number));
            exception
                when pkg_err_code.e_invalid_phone_number_value then
                    v_err_msg := pkg_label.format_wrapped(6205, pkg_label.list_label_params('value' => trim(line.phone_number)));
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            end;

            if v_phone_number is not null then
                merge into user_phone p
                using (select v_user_id as user_id,
                              v_phone_number.prefix_code as prefix_code,
                              v_phone_number.country_code as country_code,
                              v_phone_number.area_code as area_code,
                              v_phone_number.subscriber_code as subscriber_code
                         from dual) np
                  on (p.user_id = np.user_id)
                when matched then
                    update set p.prefix_code = np.prefix_code,
                               p.country_code = np.country_code,
                               p.area_code = np.area_code,
                               p.subscriber_code = np.subscriber_code
                when not matched then
                    insert (user_id, prefix_code, country_code, area_code, subscriber_code, program_id)
                    values (np.user_id, np.prefix_code, np.country_code, np.area_code, np.subscriber_code, v_pid);
            end if;
        end if;

        -- assign sec roles
        if (not pkg_imp_utils.is_empty_str(line.secrole1)) then
            pkg_user.user_sec_role_insert(v_user_id, pkg_ext_imp_utils.get_sec_role_id(line.secrole1, p_rid, line.row_num));
        end if;

        if (not pkg_imp_utils.is_empty_str(line.secrole2)) then
            pkg_user.user_sec_role_insert(v_user_id, pkg_ext_imp_utils.get_sec_role_id(line.secrole2, p_rid, line.row_num));
        end if;

        if (not pkg_imp_utils.is_empty_str(line.secrole3)) then
            pkg_user.user_sec_role_insert(v_user_id, pkg_ext_imp_utils.get_sec_role_id(line.secrole3, p_rid, line.row_num));
        end if;

        for rec in (select column_value as sec_role
                      from table(pkg_str.split_string_to_table(line.sec_roles))) loop
            pkg_user.user_sec_role_insert(v_user_id, pkg_ext_imp_utils.get_sec_role_id(rec.sec_role, p_rid, line.row_num));
        end loop;

        assign_menu(line.menu, v_user_id, p_rid, line.row_num);
        assign_global_views(line.globalviews, v_user_id);
        assign_global_filters(line.filters, v_user_id);
        assign_discp(line.discip1, v_user_id, p_rid, line.row_num);
        assign_discp(line.discip2, v_user_id, p_rid, line.row_num);
        assign_discp(line.discip3, v_user_id, p_rid, line.row_num);

        if (p_update_xitors = 1) then
            v_user_xid := get_user_xitor_pk(p_user_ttid, v_user_id, v_is_new, p_xkey_val);

            -- import config fields data
            imp_cfields_data(v_user_xid, line.row_num, p_user_ttid, p_rid);
        end if;

        --Exec user trigger
        begin
            if (v_new_user) then
                v_err_type := pkg_imp_run.c_et_new_user_rule;
                v_rt := 'New User';
                v_ret_str := pkg_ruleator.execute_trigger(24, null, v_user_id);
            else
                v_err_type := pkg_imp_run.c_et_update_user_rule;
                v_rt := 'Update User';
                v_ret_str := pkg_ruleator.execute_trigger(25, null, v_user_id);
            end if;

            if (v_retval is not null) then
                raise_application_error(-20000, v_retval);
            end if;
        exception
            when others then
                v_err_msg := 'Error when executing "'|| v_rt || '" rule. ';
                v_err_msg := v_err_msg || 'USER_ID' || ' = ' || v_user_id ;
                v_err_msg := v_err_msg || chr(10) || v_retval || chr(10) || sqlerrm;
                pkg_imp_run.write_error(p_rid, v_err_msg, v_err_type, line.row_num);
                rollback;
                return;
        end;

        if (p_update_xitors = 1) then
            --Exec trackor trigger
            begin
                if (v_is_new) then
                    v_err_type := pkg_imp_run.c_et_new_xitor_rule;
                    v_rt := 'New Trackor';
                    v_retval := pkg_ruleator.execute_trigger(18, p_user_ttid, v_user_xid);
                else
                    v_err_type := pkg_imp_run.c_et_update_xitor_rule;
                    v_rt := 'Update Trackor';
                    v_retval := pkg_ruleator.execute_trigger(19, p_user_ttid, v_user_xid);
                end if;

                if (v_retval is not null) then
                    raise_application_error(-20000, v_retval);
                end if;
            exception
                when others then
                    v_err_msg := 'Error when executing "'|| v_rt || '" rule. ';
                    v_err_msg := v_err_msg || 'TRACKOR_ID' || ' = ' || v_user_xid ;
                    v_err_msg := v_err_msg || chr(10) || v_retval || chr(10) || dbms_utility.format_error_stack|| chr(10) || dbms_utility.format_error_backtrace;
                    pkg_imp_run.write_error(p_rid, v_err_msg, v_err_type, line.row_num);
                    rollback;
                    return;
            end;
        end if;

        if pkg_imp_utils.convert_boolean(line.init_password_reset) = 1 then
            v_secret_key := dbms_random.string('x', pkg_user.c_secret_key_length);
            v_secret_key_encrypt := pkg_vqsecurity.encrypt(v_secret_key);
            pkg_user.init_password_reset_and_email(v_user_id, v_secret_key, v_secret_key_encrypt, 0);
        end if;

        commit;
    exception
        when others then
            v_err_msg := 'Username: "' || line.un || '"' || chr(10) || dbms_utility.format_error_stack|| chr(10) || dbms_utility.format_error_backtrace;
            pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, line.row_num);
            rollback;
    end import_user;

    procedure assign_menu(p_app_name application.name%type,
        p_uid in users.user_id%type,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type)
    is
        v_pid number;
        v_assigned number;
        v_appid application.application_id%type;
        v_app_pid number;
    begin
        if (pkg_imp_utils.is_empty_str(p_app_name)) then
            return;
        end if;

        select program_id into v_pid from imp_run where imp_run_id = p_rid;

        select application_id, program_id
          into v_appid, v_app_pid
          from application
         where upper(name) = upper(p_app_name)
           and program_id in (v_pid, 0);

        if v_app_pid = 0 then
            --trying to assign static application menu
            return;
        end if;

        select count(*) into v_assigned
        from user_obj_lookup
        where user_id = p_uid
              and sec_group_type_id = 17
              and obj_id = v_appid;

        if v_assigned = 0 then
            --menu app to user exception
            pkg_user.user_obj_exception_modify(p_uid, v_appid, 17, 1);
        end if;
    exception
        when no_data_found then
            pkg_ext_imp_utils.log_not_found_err('APPLICATION.NAME', p_app_name, p_rid, p_row_num);
    end assign_menu;

    procedure assign_global_views(p_assign_views in varchar2,
        p_uid in users.user_id%type)
    is
        v_pid number;
    begin
        if (pkg_imp_utils.convert_boolean(p_assign_views) = 0) then
            return;
        end if;

        select program_id into v_pid from users where user_id = p_uid;

        for rec in (select v.view_opt_id
                      from view_opt v
                      join grid_page x on (v.grid_page_id = x.grid_page_id)
                     where v.user_id is null
                       and v.filter_view_type_id = pkg_filter_view.c_grid_page_type_id
                       and pkg_sec_priv_program.get_priv(p_uid, x.security_group) like 'R%'
                       and v.view_opt_id not in (select obj_id
                                                   from user_obj_lookup p
                                                  where p.sec_group_type_id = pkg_sec_group.c_view_type_id
                                                    and p.user_id = p_uid)
                       and v.program_id = v_pid)
        loop
            --user to view exception
            pkg_user.user_obj_exception_modify(p_uid, rec.view_opt_id, pkg_sec_group.c_view_type_id, 1);
        end loop;
    end assign_global_views;

    procedure assign_global_filters(p_assign_filters in varchar2,
        p_uid in users.user_id%type)
    is
        v_pid number;
    begin
        if (pkg_imp_utils.convert_boolean(p_assign_filters) = 0) then
            return;
        end if;

        select program_id into v_pid from users where user_id = p_uid;

        for rec in (select f.filter_opt_id
                      from filter_opt f
                      join grid_page x on (f.grid_page_id = x.grid_page_id)
                     where f.filter_view_type_id = pkg_filter_view.c_grid_page_type_id
                       and f.user_id is null
                       and pkg_sec_priv_program.get_priv(p_uid, x.security_group) like 'R%'
                       and f.filter_opt_id not in (select obj_id
                                                     from user_obj_lookup p
                                                    where p.sec_group_type_id = pkg_sec_group.c_filter_type_id
                                                      and p.user_id = p_uid)
                       and f.program_id = v_pid)
        loop
            --user to filter exception
            pkg_user.user_obj_exception_modify(p_uid, rec.filter_opt_id, pkg_sec_group.c_filter_type_id, 1);
        end loop;
    end assign_global_filters;

    procedure assign_discp(p_discp v_discp.discp_type%type,
        p_uid in users.user_id%type,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type)
    is
        v_assigned number;
        v_discp_id v_discp.discp_id%type;
        v_pid number;
    begin
        if (pkg_imp_utils.is_empty_str(p_discp)) then
            return;
        end if;

        select program_id into v_pid from imp_run where imp_run_id = p_rid;

        select discp_id into v_discp_id
        from v_discp
        where discp_type = p_discp
              and program_id = v_pid;

        select count(*) into v_assigned
        from user_obj_lookup
        where sec_group_type_id = 16
              and user_id = p_uid
              and obj_id = v_discp_id;

        if v_assigned = 0 then
            --user to discp exception
            pkg_user.user_obj_exception_modify(p_uid, v_discp_id, 16, 1);
        end if;
    exception
        when no_data_found then
            pkg_ext_imp_utils.log_not_found_err('V_DISCP.DISCP_TYPE', p_discp, p_rid, p_row_num);
    end assign_discp;

    function get_user_xitor_pk(p_user_ttid xitor_type.xitor_type_id%type,
        p_uid in users.user_id%type,
        p_is_new in out boolean,
        p_xkey_val in xitor.xitor_key%type)
    return xitor.xitor_id%type
    is
        v_xid xitor.xitor_id%type;
        v_xitor_key xitor.xitor_key%type;
        v_pid users.program_id%type;
        v_un users.un%type;
    begin
        select un, program_id into v_un, v_pid from users where user_id = p_uid;

        begin
            select xitor_id into v_xid
            from users
            where user_id = p_uid
                  and xitor_id is not null;
        exception
            when no_data_found then
                v_xid := null;
        end;

        if (v_xid is null) then
            if (p_xkey_val is null) then
                v_xitor_key := v_un;
                v_xid := pkg_xitor.new_xitor(p_user_ttid, v_xitor_key, 0, null, p_uid, v_pid, null);

                v_xitor_key := pkg_xitor.generate_xitor_key(v_xid);
                if (v_xitor_key is not null) then
                    update xitor set xitor_key = v_xitor_key where xitor_id = v_xid;
                end if;
            else
                v_xid := pkg_xitor.new_xitor(p_user_ttid, p_xkey_val, 0, null, p_uid, v_pid, null);
            end if;

            update users set xitor_id = v_xid where user_id = p_uid;

            p_is_new := true;
        else
            p_is_new := false;
        end if;

        return v_xid;
    end get_user_xitor_pk;

    procedure imp_cfields_data(p_xid in xitor.xitor_id%type,
        p_row_num in number,
        p_xt_id in xitor_type.xitor_type_id%type,
        p_rid in imp_run.imp_run_id%type)
    is
        v_date_format imp_spec.date_format%type;
        v_time_format imp_spec.time_format%type;
        v_cfid config_field.config_field_id%type;
        v_cftype config_field.data_type%type;
        v_table_id config_field.attrib_v_table_id%type;
        v_vtab_val_id attrib_v_table_value.attrib_v_table_value_id%type;
        v_prog_id program.program_id%type;
        v_err_msg clob;
        v_line_num_col number;
        v_line_num number;
        v_cfname config_field.config_field_name%type;
        v_col_parent_id xitor_type.xitor_type_id%type;
        v_relation_id relation.relation_type_id%type;
        v_parent_field varchar2(50);
        v_parent_id xitor.xitor_id%type;
        v_val_clob clob;
        v_start_row imp_run.start_row%type;
    begin
        v_date_format := pkg_ext_imp_utils.get_date_format(p_rid);
        v_time_format := pkg_ext_imp_utils.get_time_format(p_rid);

        select p.program_id, r.start_row
          into v_prog_id, v_start_row
          from process p, imp_run r
         where p.process_id = r.process_id
           and r.imp_run_id = p_rid;

        for rec_data in cur_cf_data(p_rid, p_row_num)
        loop
            begin
                v_cfname := pkg_imp_run.cell_value(p_rid, v_start_row, rec_data.col_num);

                -- Check to see if the column name is a relational parent of this
                get_parent_type_info(p_xt_id, v_cfname, v_col_parent_id, v_parent_field, v_relation_id);
                if (v_col_parent_id is not null) then
                    select xitor_id into v_parent_id
                    from xitor
                    where xitor_type_id = v_col_parent_id
                          and program_id = v_prog_id
                          and xitor_key = trim(rec_data.data);

                    pkg_relation.new_relation(v_parent_id,p_xid,v_relation_id);
                else
                    select config_field_id, data_type, attrib_v_table_id into v_cfid, v_cftype, v_table_id
                    from config_field
                    where config_field_name = v_cfname
                          and xitor_type_id = p_xt_id
                          and (is_static = 0 or config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id));

                    v_line_num_col := pkg_imp_utils.get_col_num(p_rid,'LINE_NUM');
                    if (v_line_num_col is null) then
                        v_line_num := 1;
                    else
                        v_line_num := pkg_imp_run.cell_value(p_rid, p_row_num, v_line_num_col);
                        v_line_num := nvl(v_line_num, 1);
                    end if;

                    if (v_cftype in (4, 10)) then
                        v_vtab_val_id :=pkg_dl_support.get_vtabid(v_table_id, v_prog_id, rec_data.data, true);
                        v_val_clob := to_char(v_vtab_val_id);
                        pkg_ext_imp_utils.set_cf_data(v_cfid, p_xid, v_date_format, v_time_format, v_val_clob, v_line_num, 1);
                    else
                        pkg_ext_imp_utils.set_cf_data(v_cfid, p_xid, v_date_format, v_time_format, rec_data.data, v_line_num);
                    end if;
                end if;
            exception
                when others then
                    v_err_msg := 'Trackor_id: "' || p_xid || '" ';
                    v_err_msg := v_err_msg || 'column: "' || v_cfname;
                    v_err_msg := v_err_msg || '"' || chr(10) || dbms_utility.format_error_stack|| chr(10) || dbms_utility.format_error_backtrace;
                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_data, p_row_num);
            end;
        end loop;
    end imp_cfields_data;

    procedure get_parent_type_info(p_child_type_id in number,
        p_col_name in varchar2,
        p_parent_type_id out number,
        p_parent_type_col out varchar2,
        p_relation_type_id out number)
    as
        v_parent_type_id number;
        v_col_name varchar2(2000);
        v_parent_col varchar2(50);
        v_relation_type_id number;
    begin
        v_col_name := trim(p_col_name);
        for rec in (select b.relation_type_id, p.xitor_type_id,p.XITOR_TYPE,lid.label_text id_label
                    from relation_type b
                         join xitor_type p on (b.parent_type_id = p.xitor_type_id)
                         left outer join vw_label lid on (p.xitorid_label_id = lid.label_id and lid.app_lang_id = 1)
                    where b.CHILD_TYPE_ID = p_child_type_id)
        loop
            if (v_col_name = rec.xitor_type) then
                v_parent_type_id := rec.xitor_type_id;
                v_parent_col := 'TYPE';
                v_relation_type_id := rec.relation_type_id;
            elsif (v_col_name = rec.id_label) then
                v_parent_type_id := rec.xitor_type_id;
                v_parent_col := 'ID';
                v_relation_type_id := rec.relation_type_id;
            end if;
        exit when v_parent_type_id is not null;
        end loop;

        p_parent_type_id := v_parent_type_id;
        p_parent_type_col := v_parent_col;
        p_relation_type_id := v_relation_type_id;
    end;

end pkg_ext_imp_user;
/