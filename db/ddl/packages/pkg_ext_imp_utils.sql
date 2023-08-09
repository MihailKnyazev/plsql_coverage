CREATE OR REPLACE PACKAGE BODY PKG_EXT_IMP_UTILS 
/*
 * Copyright 2003-2021 OneVizion, Inc. All rights reserved.
 */
as

    function get_sec_role_id(p_role_type in sec_role.role_type%type,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type)
    return sec_role.sec_role_id%type
    as
        v_result sec_role.sec_role_id%type;
        v_pid number;
    begin
        select program_id into v_pid from imp_run where imp_run_id = p_rid;
        /*TODO: sec_role table contains unique index based on role_type, program_id, team_id.
          If user adds new sec role with different team_id and within the same program, all the imports which use this method will be failed.*/
        select sec_role_id into v_result from sec_role
        where role_type = p_role_type and program_id = v_pid;
        return v_result;
    exception
        when others then
            log_not_found_err('ROLE_TYPE', p_role_type, p_rid, p_row_num);
            return null;
    end get_sec_role_id;

    procedure log_not_found_err(p_field_name in varchar2,
        p_field_value in varchar2,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in imp_run_grid.row_num%type)
    is
        v_err_msg imp_run_error.error_msg%type;
    begin
        v_err_msg := p_field_name || ' "' || p_field_value || '" not found';
        pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_et_ext_not_found, p_row_num);
    end log_not_found_err;

    function get_date_format(p_rid in imp_run.imp_run_id%type)
    return imp_spec.date_format%type
    is
        v_date_format imp_spec.date_format%type;
    begin
        select date_format into v_date_format
        from imp_spec
        where imp_spec_id = (select imp_spec_id
                             from imp_run
                             where imp_run_id = p_rid);

        return v_date_format;
    end get_date_format;

    function get_time_format(p_rid in imp_run.imp_run_id%type)
    return imp_spec.time_format%type
    is
        v_time_format imp_spec.time_format%type;
    begin
        select time_format into v_time_format
        from imp_spec
        where imp_spec_id = (select imp_spec_id
                             from imp_run
                             where imp_run_id = p_rid);

        return v_time_format;
    end get_time_format;

    procedure set_cf_data(cfid number,
        entid number,
        p_date_format in imp_spec.date_format%type,
        p_time_format in imp_spec.time_format%type,
        val in out nocopy clob,
        ln number default 1,
        idval number default 0)
    as
    pragma autonomous_transaction;
        dt number;
        v_val_str varchar2(100);
    begin
        pkg_lob_utils.replace_clob(val, '''''', '''');

        if pkg_dl_support.AllowNulls = 1 and val = 'NULL' then
            val := null;
        end if;

        select data_type into dt from config_field
        where config_field_id = cfid;

        if (dt = 2) then
            pkg_dl_support.set_cf_data(cfid, entid, to_date(val, p_date_format), ln, idval);
        elsif (dt = 90) then
            pkg_dl_support.set_cf_data(cfid, entid, to_date(val, p_date_format || ' ' || p_time_format), ln);
        elsif (dt = 91) then
            pkg_dl_support.set_cf_data(cfid, entid, to_date('01-01-1970', 'mm-dd-yyyy') + (to_date(val, p_time_format) - trunc(to_date(val, p_time_format))), ln);
        elsif (dt = 3) then
            v_val_str := pkg_imp_utils.convert_boolean(val);
            pkg_dl_support.set_cf_data(cfid, entid, v_val_str, ln, idval);
        else
            pkg_dl_support.set_cf_data(cfid, entid, val, ln, idval);
        end if;

        commit;
    end set_cf_data;

end pkg_ext_imp_utils;
/