CREATE OR REPLACE PACKAGE BODY PKG_EXT_IMP 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
  cursor cur_task_id is select wp_task_id from wp_tasks;
  type ct_task_id is ref cursor return cur_task_id%rowtype;

  type t_task_col_name is table of boolean index by varchar2(255 char);
  v_list_task_col_name t_task_col_name;

    c_imp_label_system constant number := 1;
    c_imp_label_program constant number := 2;
    c_imp_label_task constant number:= 3;

    c_task_order_number_pattern constant varchar2(20) := '\d+(\.\d{1,2})?$';

  ---------------------------------------------------------------
  -- Utility functions declarations
  ---------------------------------------------------------------
  function is_column_name(p_rid in imp_run.imp_run_id%type, p_name in varchar2)
    return boolean;

  /**
   * Check if config field exists for xitor type other then specified in p_xtid
   */
  function is_col_for_other_xt(
    p_rid in imp_run.imp_run_id%type,
    p_name in varchar2,
    p_xtid in xitor_type.xitor_type_id%type)
    return boolean;

  /**
   * Search wp_task_id for wp by task order number. For sub-xtior tasks it
   * uses value of config field to find sub-xitor and it's task_id
   *
   * @param p_wpid
   * @param p_task_order_num
   * @param p_cfid id of config field which will be used to search sub-xitor tasks
   * @param p_cf_value value of config field
   * @param p_rid
   */
  function get_subxitor_task_id_by_cf_val(
    p_wpid in wp_workplan.wp_workplan_id%type,
    p_task_order_num in wp_tasks.order_number%type,
    p_cfid in config_field.config_field_id%type,
    p_cf_value in varchar2,
    p_rid in imp_run.imp_run_id%type) return ct_task_id;

  function get_subxitor_task_id_by_key(
    p_wpid in wp_workplan.wp_workplan_id%type,
    p_task_order_num in wp_tasks.order_number%type,
    p_sub_xitor_key in xitor.xitor_key%type,
    p_rid in imp_run.imp_run_id%type) return wp_tasks.wp_task_id%type;

  /**
   * Parse task date string and extract order number
   */
  function get_task_order_num(
    p_col_name in varchar2)
    return wp_tasks.order_number%type;


  procedure set_date_task(
    p_tid wp_tasks.wp_task_id%type,
    v_dtfmt varchar2,
    v_date_col varchar2,
    p_value varchar2,
    p_calc_dates number,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in imp_run_grid.row_num%type);

  function get_date_type_id(
    p_tid in wp_tasks.wp_task_id%type,
    p_date_type varchar2)
    return v_wp_task_date_type.wp_task_date_type_id%type;

  /**
   * Check if p_col_name is correct task column name. It should start with one
   * of P or A or B or R latter followed by S or F followed by task order number
   */
  function is_task_col_name_valid(p_col_name varchar2) return boolean;

  /**
   * Import all dates for single task and trackor, trigger rules if p_calc_dates <> 1
   */
  procedure import_tasks(
    p_rid imp_run.imp_run_id%type,
    p_sub_xitor_key_col_num number,
    p_cfid config_field.config_field_id%type,
    p_cf_value varchar2,
    p_calc_dates number,
    p_wp_id wp_workplan.wp_workplan_id%type,
    p_row_num imp_run_grid.row_num%type);


  ---------------------------------------------------------------
  -- Public functions implementations
  ---------------------------------------------------------------

  procedure WpDatesByOrderNum(
    p_rid in imp_run.imp_run_id%type,
    p_xt_id in xitor_type.xitor_type_id%type,
    p_sub_xitor_key_col_num in number default null,
    p_cfid in config_field.config_field_id%type default null,
    p_cf_value in varchar2 default null,
    p_calc_dates in number default 0) as

    v_errmsg clob;
    v_rowcount number := 0;

    e_trackor_not_exists exception;
    e_wp_not_exists exception;
    pragma exception_init(e_trackor_not_exists, -21000);
    pragma exception_init(e_wp_not_exists,      -21001);

    cursor cur_pk(p_rid in imp_run.imp_run_id%type) is
        with imp_data as (
            select /*+materialize*/ x.row_num, trim(x.data) as xitor_key, trim(y.data) as wpname
              from (select row_num, data from imp_run_grid where imp_run_id = p_rid and col_num = 1) x,
                   (select row_num, data from imp_run_grid where imp_run_id = p_rid and col_num = 2) y
             where x.row_num = y.row_num and x.row_num > (select start_row
                                                            from imp_run
                                                           where imp_run_id = p_rid))   
        select wp.wp_workplan_id, imp_data.row_num, imp_data.xitor_key, imp_data.wpname, x.xitor_id
          from imp_data 
          left join xitor x on x.xitor_key = imp_data.xitor_key
                           and x.xitor_type_id = p_xt_id 
          left join wp_workplan wp on wp.name = imp_data.wpname
                                  and wp.xitor_id = x.xitor_id
                                  and wp.workplan_type = 1
                                  and wp.active = 1;
    begin
        pkg_imp_run.drop_pks(p_rid);

        for rec_pk in cur_pk(p_rid) loop
            begin
                if rec_pk.xitor_id is null then
                    raise e_trackor_not_exists;
                elsif rec_pk.wp_workplan_id is null then
                    raise e_wp_not_exists;
                end if;

                pkg_imp_run.add_pk(p_rid, null, rec_pk.row_num, rec_pk.wp_workplan_id, 0);

                -- import row task by task
                import_tasks(p_rid, p_sub_xitor_key_col_num, p_cfid, p_cf_value, p_calc_dates,
                             rec_pk.wp_workplan_id, rec_pk.row_num);
            exception
                when e_trackor_not_exists then
                    v_errmsg := 'Trackor not found for Trackor Key:"' || rec_pk.xitor_key || '"';
                    pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_pk, rec_pk.row_num);
                when e_wp_not_exists then
                    v_errmsg := 'Workplan not found for Trackor Key:"' || rec_pk.xitor_key || '" Workplan:"' || rec_pk.wpname || '"';
                    pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_pk, rec_pk.row_num);
                when others then
                    v_errmsg := 'Unexpected error for Trackor Key:"' || rec_pk.xitor_key || '" Workplan:"' || rec_pk.wpname || '"' || chr(10) || sqlerrm;
                    pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_pk, rec_pk.row_num);
            end;

            v_rowcount := v_rowcount + 1;
            pkg_imp_run.set_rows(p_rid, v_rowcount);
        end loop;
    end WpDatesByOrderNum;


  procedure WpDatesByOrderNumComplex(
    p_rid imp_run.imp_run_id%type,
    p_sub_xitor_key_col_num number default null,
    p_cfid config_field.config_field_id%type default null,
    p_cf_value varchar2 default null,
    p_calc_dates number default 0)
  as
    v_rowcount number := 0;
    v_process_id number;

    cursor cur_pk(p_rid in imp_run.imp_run_id%type) is
      select irepk.row_num, irepk.pk
      from imp_run irun
        join imp_spec ispec on (irun.imp_spec_id = ispec.imp_spec_id)
        join imp_entity ient on (ispec.imp_spec_id = ient.imp_spec_id and ient.entity_name = 'Workplan')
        join imp_run_entity_pk irepk on (irun.imp_run_id = irepk.imp_run_id)
      where irun.imp_run_id = p_rid;
    begin
      select process_id into v_process_id
      from imp_run
      where imp_run_id = p_rid;

      pkg_imp_run.drop_pks(p_rid);
      pkg_imp_run.fill_entity_pks(v_process_id, p_rid);

      for rec_pk in cur_pk(p_rid) loop
        -- import row task by task
        import_tasks(p_rid, p_sub_xitor_key_col_num, p_cfid, p_cf_value, p_calc_dates, rec_pk.pk, rec_pk.row_num);
        v_rowcount := v_rowcount + 1;
        pkg_imp_run.set_rows(p_rid, v_rowcount);
      end loop;
    end WpDatesByOrderNumComplex;


  procedure import_tasks(
    p_rid imp_run.imp_run_id%type,
    p_sub_xitor_key_col_num number,
    p_cfid config_field.config_field_id%type,
    p_cf_value varchar2,
    p_calc_dates number,
    p_wp_id wp_workplan.wp_workplan_id%type,
    p_row_num imp_run_grid.row_num%type)
  is
    pragma autonomous_transaction;

    v_errmsg clob;
    v_dt imp_run_grid.data%type;
    v_sub_xitor_key imp_run_grid.data%type;
    v_task_id wp_tasks.wp_task_id%type;
    v_templ_task_id wp_tasks.template_task_id%type;
    v_rule_retval varchar2(4000);
    cur_task_id ct_task_id;
    v_sub_xitor_key_col_num number;
    v_sub_xitor_key_col_num2 number;
    v_dtfmt imp_spec.date_format%type;
    v_task_name wp_tasks.task_name%type;
    v_is_task_col_name_valid boolean;

    type t_array_wp_task_id is table of wp_tasks.wp_task_id%type index by pls_integer;
    v_array_wp_task_id t_array_wp_task_id;

    cursor cur_tasks(p_rid in imp_run.imp_run_id%type, p_sub_xitor_key_col_num in number) is
      select distinct to_number(regexp_substr(g.data, c_task_order_number_pattern)) as task_order_num,
             g.col_num, g.data
        from imp_run_grid g
       where g.imp_run_id = p_rid
         and g.col_num > 2
         and g.row_num = (select start_row
                            from imp_run
                           where imp_run_id = p_rid) 
         and g.col_num <> p_sub_xitor_key_col_num
         and regexp_instr(regexp_substr(g.data, c_task_order_number_pattern), '^[\d]') = 0
         and upper(g.data) not in (
              select upper(col.name) from imp_column col
                join imp_run r on (r.imp_spec_id = col.imp_spec_id)
               where r.imp_run_id = p_rid)
       order by col_num asc;

    i pls_integer;   
  begin
        -- can't use nvl(p_sub_xitor_key_col_num, -1) in query because of
        -- incorrect data type convertion
        v_sub_xitor_key_col_num := nvl(p_sub_xitor_key_col_num, -1);
        v_sub_xitor_key_col_num2 := nvl(p_sub_xitor_key_col_num, 1);
        v_dtfmt := pkg_ext_imp_utils.get_date_format(p_rid);

        for rec_col in cur_tasks(p_rid, v_sub_xitor_key_col_num) loop
            begin
                v_is_task_col_name_valid := v_list_task_col_name(rec_col.data);
            exception
                when no_data_found then
                    v_list_task_col_name(rec_col.data) := is_task_col_name_valid(rec_col.data);
                    v_is_task_col_name_valid := v_list_task_col_name(rec_col.data);
            end;

            if (not(v_is_task_col_name_valid)) then
                v_errmsg := '"' || rec_col.data || '" incorrect column name.';
                pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_data, null);

            else
                begin
                    select dt.data, sxkey.data into v_dt, v_sub_xitor_key
                      from (select row_num, data from imp_run_grid
                             where imp_run_id = p_rid and col_num = rec_col.col_num) dt,
                           (select row_num, data from imp_run_grid
                             where imp_run_id = p_rid
                               and col_num = v_sub_xitor_key_col_num2) sxkey
                      where dt.row_num = p_row_num and sxkey.row_num = p_row_num
                        and dt.data is not null;
                exception
                    when no_data_found then
                        --date column is empty in CSV
                        continue;
                end; 

                begin
                    if (p_sub_xitor_key_col_num is not null) then
                        v_task_id := get_subxitor_task_id_by_key(p_wp_id, rec_col.task_order_num, v_sub_xitor_key, p_rid);

                        if (v_task_id is not null) then
                            set_date_task(v_task_id, v_dtfmt, rec_col.data,
                                          v_dt, p_calc_dates, p_rid, p_row_num);
                        end if;
                    else
                        -- we are using ref cursor, because more than one
                        -- task can be found by sub-xitor's cf value
                        -- here the cursor is opened
                        cur_task_id := get_subxitor_task_id_by_cf_val(p_wp_id, rec_col.task_order_num, p_cfid, p_cf_value, p_rid);

                        loop
                            fetch cur_task_id bulk collect into v_array_wp_task_id limit 10000;

                            i := v_array_wp_task_id.first;

                            while (i is not null) loop
                                v_task_id := v_array_wp_task_id(i);
                                set_date_task(v_task_id, v_dtfmt, rec_col.data, v_dt, p_calc_dates, p_rid, p_row_num);

                                i := v_array_wp_task_id.next(i);
                            end loop;

                            exit when cur_task_id%notfound;
                        end loop;

                        close cur_task_id;
                    end if;

                    if (p_calc_dates <> 1) then
                        select template_task_id, task_name into v_templ_task_id, v_task_name
                          from wp_tasks where wp_task_id = v_task_id;

                        --TODO how to handle situation when get_subxitor_task_id_by_cf_val
                        --return multiple task ids?
                         v_rule_retval := pkg_ruleator.execute_trigger(1, v_templ_task_id, v_task_id);
                    end if;

                    commit;
                exception
                    when others then
                        if cur_task_id%isopen then 
                            close cur_task_id; 
                        end if;

                        v_errmsg := 'Exception during processing task. Order Number: "'
                                  || rec_col.task_order_num || '"' || chr(10) || 'Workplan ID: "'
                                  || p_wp_id || '"' || chr(10) || dbms_utility.format_error_stack || chr(10)
                                  || dbms_utility.format_error_backtrace;

                        pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_data, p_row_num);
                        rollback;
                end;
            end if;

        end loop;  --Column Loop

  end import_tasks;

  function is_task_col_name_valid(p_col_name varchar2) return boolean
  is
      v_conf_date integer;
      v_date_abbr varchar2(20);
      v_date_type varchar2(20);
      v_sf varchar2(1);
      v_ret boolean;
  begin
      v_date_abbr := substr(p_col_name, 1, regexp_instr(p_col_name, c_task_order_number_pattern) - 1);
      v_date_type := substr(v_date_abbr, 1, length(v_date_abbr) - 1);
      v_sf := substr(v_date_abbr, length(v_date_abbr));

      select count(*) into v_conf_date 
        from v_wp_task_date_type dt
        join vw_label l on (dt.abbr_app_label_id = l.label_id)
       where l.label_text = v_date_type and dt.program_id = pkg_sec.get_pid;

      if (((v_date_type in('P', 'A', 'B', 'R') or v_conf_date > 0) and (v_sf in('S', 'F')))
          or (v_date_abbr in ('NA', 'BlockCalc')))
         and (get_task_order_num(p_col_name) is not null) then
         v_ret := true;
      else
         v_ret := false;
      end if;

      return v_ret;
  end is_task_col_name_valid;


  /* Deprecated */
  procedure ConfiguredFieldLoad(rid imp_run.imp_run_id%type)
  as
    begin
      pkg_ext_imp_field.ConfiguredFieldLoad(rid);
    end ConfiguredFieldLoad;

  procedure SecRoleLoad(rid in imp_run.imp_run_id%type) as
    rowcount number := 0;
    v_sec_role_id sec_role.sec_role_id%type;
    v_sec_group_prog_id number;
    v_sec_group_sys_id number;
    v_err_msg clob;
    begin
      for line in (select sec_role.row_num, 
                          sec_role.data as srole,
                          sec_group.data as sgroup, 
                          priv.data as privs 
                     from (select row_num, data
                             from imp_run_grid 
                            where imp_run_id = rid
                              and col_num = pkg_imp_utils.get_col_num(rid, 'ROLE_TYPE')
                              and data is not null) sec_role, 
                          (select row_num, data
                             from imp_run_grid 
                             where imp_run_id = rid 
                               and col_num = pkg_imp_utils.get_col_num(rid, 'SECURITY_GROUP') 
                               and data is not null) sec_group,
                          (select row_num, data 
                             from imp_run_grid where imp_run_id = rid
                              and col_num = pkg_imp_utils.get_col_num(rid, 'PRIV')
                              and data is not null) priv
                    where sec_role.row_num > (select start_row
                                                from imp_run
                                               where imp_run_id = rid)
                      and sec_role.row_num = priv.row_num(+)
                      and sec_role.row_num = sec_group.row_num)
      loop
        begin
          v_sec_role_id := pkg_ext_imp_utils.get_sec_role_id(line.srole, rid, line.row_num);

          begin
            select sec_group_system_id into v_sec_group_sys_id from sec_group_system
            where security_group = line.sgroup;
            exception when no_data_found then
            v_sec_group_sys_id := null;
          end;

          begin
            select sec_group_program_id into v_sec_group_prog_id from sec_group_program
            where security_group = line.sgroup and program_id = pkg_sec.get_pid();
            exception when no_data_found then
            v_sec_group_prog_id := null;
          end;

          if (v_sec_group_sys_id = v_sec_group_prog_id) then
            v_err_msg := 'Security group [' || line.sgroup || '] is defined as a system and as a program.';
            pkg_imp_run.write_error(rid, v_err_msg, pkg_imp_run.c_et_ext_not_found, line.row_num);
          elsif ((v_sec_group_sys_id is not null) and (v_sec_role_id is not null)) then
            pkg_sec_priv_system.set_priv_by_id(v_sec_role_id, v_sec_group_sys_id, line.privs);
          elsif ((v_sec_group_prog_id is not null) and (v_sec_role_id is not null)) then
            pkg_sec_priv_program.set_priv_by_id(v_sec_role_id, v_sec_group_prog_id, line.privs);
          else
            pkg_ext_imp_utils.log_not_found_err('SECURITY_GROUP', line.sgroup, rid, line.row_num);
          end if;

          rowcount := rowcount + 1;
          pkg_imp_run.set_rows(rid, rowcount);
          exception when others then
          v_err_msg := 'v_sec_role_id = ' || v_sec_role_id || chr(13)
                       || 'v_sec_group_system_id = ' || v_sec_group_prog_id || chr(13)
                       || 'v_sec_group_program_id = ' || v_sec_group_prog_id || chr(13)
                       || 'security_group = ' || line.sgroup;
          pkg_imp_run.write_error(rid, sqlerrm || chr(13) || v_err_msg, pkg_imp_run.c_et_data, line.row_num);
        end;
      end loop;
      commit;
    end SecRoleLoad;

    function char_to_number(p_value in varchar2) return number deterministic
    as
        v_number number;
    begin
        v_number := to_number(p_value);
        return v_number;
    exception when others then 
        return null;
    end char_to_number;

  procedure XitorConfiguredFieldLoad(
      rid in imp_run.imp_run_id%type,
      p_xt_id in xitor_type.xitor_type_id%type default null,
      p_is_search_cf_by_label in number default 0,
      p_log_error in number default 0) is

      v_date_format imp_spec.date_format%type;
      v_time_format imp_spec.time_format%type;
      v_cfid config_field.config_field_id%type;
      v_rowcnt number := 0;
      v_ln_col_num number;
      v_errmsg clob;
      v_process_id number;
      v_cells_processed number := 0;

      cursor cur_col_num(p_rid in imp_run.imp_run_id%type) is
        --select column for external import exclude cols for standard mapping.
        select col_num, data as col_name,
               count(1) over() as cols_cnt
          from imp_run_grid irg
         where imp_run_id = p_rid
           and row_num = (select start_row
                            from imp_run
                           where imp_run_id = p_rid)
           and data <> 'LINE_NUM'
           and not exists (select 1
                             from imp_column ic, imp_run ir
                            where ic.imp_spec_id = ir.imp_spec_id
                              and ir.imp_run_id = p_rid
                              and upper(ic.name) = upper(irg.data));

      cursor cur_line(
          p_rid in imp_run.imp_run_id%type, 
          p_data_col_num in number,
          p_line_number_col_num in number) 
      is
        select pk.row_num, pk.pk, dt.data as dt, x.xitor_type_id,
               nvl(ln.line_number, 1) as ln
          from (select row_num, pk 
                  from imp_run_entity_pk
                 where imp_run_id = p_rid) pk, 
               xitor x,
               (select row_num, nvl(clob_data, data) as data 
                  from imp_run_grid
                 where imp_run_id = p_rid and col_num = p_data_col_num) dt,
               (select row_num, char_to_number(data) as line_number 
                  from imp_run_grid
                 where imp_run_id = p_rid and col_num = p_line_number_col_num) ln
         where pk.row_num = dt.row_num 
           and pk.row_num = ln.row_num(+)
           and dt.data is not null
           and x.xitor_id = pk.pk
         order by pk.row_num;

      cursor cur_line_static_xt(
          p_rid in imp_run.imp_run_id%type,
          p_data_col_num in number,
          p_line_number_col_num in number,
          p_xt_id in xitor_type.xitor_type_id%type)
      is         
        select pk.row_num, pk.pk, dt.data as dt, p_xt_id as xitor_type_id, 
               nvl(ln.line_number, 1) as ln
          from (select row_num, pk 
                  from imp_run_entity_pk
                 where imp_run_id = p_rid) pk,
               (select row_num, nvl(clob_data, data) as data 
                  from imp_run_grid
                 where imp_run_id = p_rid and col_num = p_data_col_num) dt,
               (select row_num, char_to_number(data) as line_number 
                  from imp_run_grid
                 where imp_run_id = p_rid and col_num = p_line_number_col_num) ln
         where pk.row_num = dt.row_num 
           and pk.row_num = ln.row_num(+)
           and dt.data is not null
         order by pk.row_num;

      cursor cur_cfid(
          p_rid in imp_run.imp_run_id%type,
          p_name in config_field.config_field_name%type,
          p_xtid in xitor_type.xitor_type_id%type)
      is
        select f.config_field_id
          from config_field f
          join label_program l on f.app_label_id = l.label_program_id
          join xitor_type xt on xt.xitor_type_id = f.xitor_type_id
         where f.xitor_type_id = p_xtid
           and (f.is_static = 0 or f.config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id))
           and (    upper(f.config_field_name) = upper(p_name) 
                 or (p_is_search_cf_by_label = 1 and (   upper(pkg_label.get_label_system_program(xt.prefix_label_id) || l.label_program_text) = upper(p_name) 
                                                      or upper(l.label_program_text) = upper(p_name))))
           and l.app_lang_id = pkg_sec.get_lang
           and f.config_field_name not in
               (select ic.name from imp_column ic, imp_run ir
                 where ic.imp_spec_id = ir.imp_spec_id
                   and ir.imp_run_id = p_rid);

      r_spec pkg_imp_run.cur_imp_spec%rowtype;

      type rt_import_data is record(
          row_num imp_run_entity_pk.row_num%type, 
          pk imp_run_entity_pk.pk%type,  
          data clob,
          xitor_type_id xitor.xitor_type_id%type,
          line_number number);

      type t_import_data is table of rt_import_data;   
      v_import_data t_import_data;
      i pls_integer;

      type t_cfs is table of varchar2(255) index by varchar2(500);
      v_cfs t_cfs;
      v_collection_key varchar2(500);
  begin
      select process_id into v_process_id
        from imp_run
       where imp_run_id = rid;

      --Search xitor ids
      pkg_imp_run.fill_entity_pks(v_process_id, rid);

      --Import columns mapped as for regular config import (non external)
      open pkg_imp_run.cur_imp_spec(rid);
      fetch pkg_imp_run.cur_imp_spec into r_spec;
      close pkg_imp_run.cur_imp_spec;

      pkg_imp_run.fill_datamap_sql(r_spec);
      pkg_imp_run.import_data(r_spec);

      v_ln_col_num := pkg_imp_utils.get_col_num(rid, 'LINE_NUM');
      v_date_format := pkg_ext_imp_utils.get_date_format(rid);
      v_time_format := pkg_ext_imp_utils.get_time_format(rid);

      -- Import data column by column
      for rec_col in cur_col_num(rid) loop
          loop
              if (p_xt_id is null) then
                  if not cur_line%isopen then
                      open cur_line(rid, rec_col.col_num, v_ln_col_num);
                  end if;

                  fetch cur_line bulk collect into v_import_data limit 10000;
              else
                  if not cur_line_static_xt%isopen then
                      open cur_line_static_xt(
                          p_rid => rid,
                          p_data_col_num => rec_col.col_num, 
                          p_line_number_col_num => v_ln_col_num,
                          p_xt_id => p_xt_id);
                  end if;

                  fetch cur_line_static_xt bulk collect into v_import_data limit 10000;
              end if;

              exit when v_import_data.count = 0;

              i := v_import_data.first;
              while (i is not null) loop
                  v_collection_key := to_char(v_import_data(i).xitor_type_id) || rec_col.col_name;
                  --collection cache of fields ids
                  if v_cfs.exists(v_collection_key) then
                      v_cfid := v_cfs(v_collection_key);
                  else
                      open cur_cfid(rid, rec_col.col_name, v_import_data(i).xitor_type_id);
                      fetch cur_cfid into v_cfid;

                      if cur_cfid%found then
                           v_cfs(v_collection_key) := v_cfid;
                      elsif is_col_for_other_xt(rid, rec_col.col_name, v_import_data(i).xitor_type_id) then
                          --imported data assigned to field from another TT, skip field
                          --ex.  --xkey_issue, fld_issue, xkey_company, fld_company
                               --1,1,null,null
                               --null,123,2,2   --this record have incorrect data for fld_issue because this line have XKEY for company
                          v_cfid := null;
                          --field not found for entities from import
                      else
                          v_cfid := null;
                          v_errmsg := 'Configured Field "' || rec_col.col_name  ||
                                      '" is not found for xitor_type_id = ' ||
                                      v_import_data(i).xitor_type_id;

                          pkg_imp_run.write_error(p_rid => rid, 
                                                  p_msg => v_errmsg, 
                                                  p_err_type_id => pkg_imp_run.c_et_data, 
                                                  p_row_num => v_import_data(i).row_num,
                                                  p_entity_id => v_import_data(i).pk,
                                                  p_col_name => rec_col.col_name);
                      end if;

                      close cur_cfid;
                  end if;


                  if v_cfid is not null then
                      begin
                          pkg_ext_imp_utils.set_cf_data(v_cfid, v_import_data(i).pk, v_date_format, v_time_format,
                                                        v_import_data(i).data, to_number(v_import_data(i).line_number));
                      exception
                          when others then                             
                              v_errmsg := dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace;
                              pkg_imp_run.write_error(p_rid => rid, 
                                                      p_msg => v_errmsg, 
                                                      p_err_type_id => pkg_imp_run.c_et_data, 
                                                      p_row_num => v_import_data(i).row_num,
                                                      p_entity_id => v_import_data(i).pk,
                                                      p_col_name => rec_col.col_name,
                                                      p_bad_data_value => v_import_data(i).data);

                              if p_log_error = 1 then 
                                  pkg_imp_run.write_field_comment(
                                      rid, sqlerrm, v_import_data(i).row_num, v_cfid, v_import_data(i).pk, 
                                      v_import_data(i).xitor_type_id, null, v_import_data(i).data, rec_col.col_num, p_log_error);
                              end if;
                      end;
                  end if;

                  v_cells_processed := v_cells_processed + 1;

                  if mod(v_cells_processed, 100) = 0 then
                      pkg_imp_run.set_rows(rid, trunc(v_cells_processed / rec_col.cols_cnt));
                  end if;

                  v_rowcnt := v_import_data(i).row_num;
                  i := v_import_data.next(i);
              end loop;
          end loop; --Row loop

          if (p_xt_id is null) then
              close cur_line;
          else
              close cur_line_static_xt;
          end if;
      end loop;  --Column Loop

      pkg_imp_run.set_rows(rid, v_rowcnt);
      pkg_imp_run.generate_xitor_keys(rid);
      pkg_imp_run.exec_xitor_triggers(rid);
  end XitorConfiguredFieldLoad;


  procedure UsersLoad(p_rid in imp_run.imp_run_id%type,
                      p_update_xitors in number default 0,
                      p_user_xt_id in xitor_type.xitor_type_id%type default null,
                      p_key_col_name in varchar2 default null) as
    begin
      pkg_ext_imp_user.UsersLoad(p_rid, p_update_xitors, p_user_xt_id, p_key_col_name);
    end UsersLoad;

  ---------------------------------------------------------------
  -- Private functions implementations
  ---------------------------------------------------------------

  procedure set_task_na(tid number, na number) as
  pragma autonomous_transaction;
    old_na number;
    ttid number;
    wpid number;
    xtid number;
    xid number;
    dur number;
    tw number;
    bdur number;
    btw number;
    bsd date;
    bfd date;
    psd date;
    pfd date;
    asd date;
    afd date;
    cal number;
    v_new_dur number;
    v_new_task_window number;
    v_new_fpd date;
    c_wp_tasks_table_name varchar2(100) := 'WP_TASKS';
    begin
      select is_not_applicable,template_task_id,wp_workplan_id,xitor_type_id,xitor_id,duration,task_window,baseline_duration,baseline_task_window,
        start_baseline_date,finish_baseline_date,start_projected_date,finish_projected_date,start_actual_date,finish_actual_date,wp_calendar_id
      into old_na,ttid,wpid,xtid,xid,dur,tw,bdur,btw,bsd,bfd,psd,pfd,asd,afd,cal
      from wp_tasks where wp_task_id = tid;

      if (na <> old_na and asd is not null) then
        if (na = 1) then
          v_new_dur := 0;
          v_new_task_window := 0;
          v_new_fpd := psd;

        else
          v_new_dur := bdur;
          v_new_task_window := btw;
          v_new_fpd := pkg_wp.add_x_days(psd, bdur, cal);
        end if;

        update wp_tasks set
          is_not_applicable = na,
          duration = v_new_dur,
          task_window = v_new_task_window,
          finish_projected_date = v_new_fpd
        where wp_task_id = tid;

        --Log update
        if not(pkg_audit.disable_audit_log) then
          pkg_audit.log_task_changes(
              p_table_name => c_wp_tasks_table_name,
              p_column_name => 'IS_NOT_APPLICABLE',
              p_pk => tid,
              p_action => pkg_audit.c_la_update,
              p_wpid => wpid,
              p_template_task_id => ttid,
              p_xtid => xtid,
              p_xid => xid,
              p_from_number => old_na,
              p_to_number => na,
              p_from_char => null,
              p_to_char => null,
              p_from_date => null,
              p_to_date => null);

          pkg_audit.log_task_changes(
              p_table_name => c_wp_tasks_table_name,
              p_column_name => 'DURATION',
              p_pk => tid,
              p_action => pkg_audit.c_la_update,
              p_wpid => wpid,
              p_template_task_id => ttid,
              p_xtid => xtid,
              p_xid => xid,
              p_from_number => dur,
              p_to_number => v_new_dur,
              p_from_char => null,
              p_to_char => null,
              p_from_date => null,
              p_to_date => null);

          pkg_audit.log_task_changes(
              p_table_name => c_wp_tasks_table_name,
              p_column_name => 'TASK_WINDOW',
              p_pk => tid,
              p_action => pkg_audit.c_la_update,
              p_wpid => wpid,
              p_template_task_id => ttid,
              p_xtid => xtid,
              p_xid => xid,
              p_from_number => tw,
              p_to_number => v_new_task_window,
              p_from_char => null,
              p_to_char => null,
              p_from_date => null,
              p_to_date => null);

          pkg_audit.log_task_changes(
              p_table_name => c_wp_tasks_table_name,
              p_column_name => 'FINISH_PROJECTED_DATE',
              p_pk => tid,
              p_action => pkg_audit.c_la_update,
              p_wpid => wpid,
              p_template_task_id => ttid,
              p_xtid => xtid,
              p_xid => xid,
              p_from_number => null,
              p_to_number => null,
              p_from_char => null,
              p_to_char => null,
              p_from_date => pfd,
              p_to_date => v_new_fpd);
        end if;

        commit;
      end if;
    end set_task_na;


  function is_column_name(p_rid in imp_run.imp_run_id%type, p_name in varchar2) return boolean 
  is
     v_column number;
     v_ret boolean;
  begin
      select count(*) into v_column 
        from imp_column ic, imp_run ir
       where ic.imp_spec_id = ir.imp_spec_id
         and ir.imp_run_id = p_rid and upper(ic.name) = upper(p_name);

      if (v_column = 0) then
          v_ret := false;
      else
          v_ret := true;
      end if;

      return v_ret;
  end is_column_name;

  function is_col_for_other_xt(
      p_rid in imp_run.imp_run_id%type,
      p_name in varchar2,
      p_xtid in xitor_type.xitor_type_id%type)
      return boolean
  is
      v_field_exists integer;
      v_ret boolean;
  begin
      select count(*) into v_field_exists
        from imp_run r 
        join imp_entity e on (e.imp_spec_id = r.imp_spec_id)
        join config_field f on (f.xitor_type_id = e.xitor_type_id)
       where r.imp_run_id = p_rid and f.config_field_name = p_name
         and (f.is_static = 0 or f.config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id))
         and e.xitor_type_id <> p_xtid;

      if v_field_exists = 0 then
          v_ret := false;
      else
          v_ret := true;
      end if;

      return v_ret;
  end is_col_for_other_xt;

  function get_subxitor_task_id_by_cf_val(
    p_wpid in wp_workplan.wp_workplan_id%type,
    p_task_order_num in wp_tasks.order_number%type,
    p_cfid in config_field.config_field_id%type,
    p_cf_value in varchar2,
    p_rid in imp_run.imp_run_id%type) return ct_task_id
  as
    v_task_id wp_tasks.wp_task_id%type;
    v_errmsg clob;
    v_wp_xtid wp_workplan.xitor_type_id%type;
    v_task_xtid wp_tasks.xitor_type_id%type;
    v_xitor_id wp_workplan.xitor_id%type;
    v_child_exists number := 0;
    cur_task_id ct_task_id;
    v_xitor_key xitor.xitor_key%type;
    v_workplan_name wp_workplan.name%type;
    v_cf_name config_field.config_Field_name%type;

    begin
      -- check if wp xitor has sub-xitors
      select t.xitor_type_id, wp.xitor_type_id, wp.xitor_id
      into v_task_xtid, v_wp_xtid, v_xitor_id
      from wp_tasks t, wp_workplan wp
      where t.order_number = p_task_order_num and t.wp_workplan_id = p_wpid
            and wp.wp_workplan_id = p_wpid and rownum < 2;

      if (v_task_xtid <> v_wp_xtid) then
        select count(1) into v_child_exists
        from ancestor a, ancestor_type at
        where at.parent_type_id = v_wp_xtid
        and at.child_type_id = v_task_xtid
        --and a.relation_type_id = at.relation_type_id
        and a.c_xitor_type_id = at.child_type_id
        and a.parent_id = v_xitor_id;
      end if;

      if ((p_cfid is null) or
          ((v_task_xtid <> v_wp_xtid) and (v_child_exists = 0))) then

        select wp_task_id into v_task_id from wp_workplan wp, wp_tasks wt
        where wt.order_number = p_task_order_num
              and wt.wp_workplan_id = p_wpid
              and wt.wp_workplan_id = wp.wp_workplan_id;

        -- need to return data in ref cursor
        open cur_task_id for select v_task_id from dual;
      else
        -- we need to log error if task can't be found
        select wp_task_id into v_task_id from wp_tasks wt, wp_workplan wp
        where wt.order_number = p_task_order_num
              and wt.wp_workplan_id = p_wpid
              and wt.wp_workplan_id = wp.wp_workplan_id
              and (wt.xitor_type_id = wp.xitor_type_id
                   or pkg_config_field_rpt.getValStrByID(wt.xitor_id, p_cfid) = p_cf_value)
              and rownum < 2;

        open cur_task_id for
        select wp_task_id from wp_tasks wt, wp_workplan wp
        where wt.order_number = p_task_order_num
              and wt.wp_workplan_id = p_wpid
              and wt.wp_workplan_id = wp.wp_workplan_id
              and (wt.xitor_type_id = wp.xitor_type_id
                   or pkg_config_field_rpt.getValStrByID(wt.xitor_id, p_cfid) = p_cf_value);
      end if;

      return cur_task_id;
      exception when others then
      select s.xitor_key,p.name into v_xitor_key, v_workplan_name
      from wp_workplan p join xitor s on (p.xitor_id = s.xitor_id)
      where p.wp_workplan_id = p_wpid;
      select config_field_name into v_cf_name
      from config_field where config_field_id = p_cfid;
      v_errmsg := 'Can''t find task_id for Order Number:"' || p_task_order_num
                  || '"' ||'on Trackor: "'||v_xitor_key||'", Workplan: "'||
                  v_workplan_name||'" ,ConfigField: "'||v_cf_name||'" = "'||p_cf_value||'".';
      v_errmsg := v_errmsg || chr(10) || sqlerrm;
      pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_pk, null);
      return null;
    end get_subxitor_task_id_by_cf_val;

  function get_subxitor_task_id_by_key(
    p_wpid in wp_workplan.wp_workplan_id%type,
    p_task_order_num in wp_tasks.order_number%type,
    p_sub_xitor_key in xitor.xitor_key%type,
    p_rid in imp_run.imp_run_id%type) return wp_tasks.wp_task_id%type
  as
    v_task_id wp_tasks.wp_task_id%type;
    v_errmsg clob;
    v_xitor_key xitor.xitor_key%type;
    v_workplan_name wp_workplan.name%type;
    begin
      if (p_sub_xitor_key is null) then
        select wp_task_id into v_task_id from wp_workplan wp, wp_tasks wt
        where wt.order_number = p_task_order_num
              and wt.wp_workplan_id = p_wpid
              and wt.wp_workplan_id = wp.wp_workplan_id;
      else
        select wp_task_id into v_task_id
        from wp_workplan wp, wp_tasks wt
          left outer join xitor x on (
            wt.xitor_id = x.xitor_id and x.xitor_key=p_sub_xitor_key)
        where wt.order_number = p_task_order_num
              and wt.wp_workplan_id = p_wpid
              and wt.wp_workplan_id = wp.wp_workplan_id
              and (wt.xitor_type_id = wp.xitor_type_id or wt.xitor_id = x.xitor_id);
      end if;

      return v_task_id;
      exception
      when others then
      select s.xitor_key,p.name into v_xitor_key, v_workplan_name
      from wp_workplan p join xitor s on (p.xitor_id = s.xitor_id)
      where p.wp_workplan_id = p_wpid;
      v_errmsg := 'Can''t find task_id for Order Number:"' || p_task_order_num
                  || '"' ||'on Trackor: "'||v_xitor_key||'", Workplan: "'||
                  v_workplan_name||'" ,SubTrackor: "'||p_sub_xitor_key||'"';
      v_errmsg := v_errmsg || chr(10) || sqlerrm;
      pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_pk, null);
      return null;
    end get_subxitor_task_id_by_key;

  function get_task_order_num(
    p_col_name in varchar2)
    return wp_tasks.order_number%type as

    v_result wp_tasks.order_number%type;
    begin
      v_result := to_number(regexp_substr(p_col_name, c_task_order_number_pattern));
      return v_result;
      exception
      when others then
      return null;
    end get_task_order_num;

  procedure set_date_task(
    p_tid wp_tasks.wp_task_id%type,
    v_dtfmt varchar2,
    v_date_col varchar2,
    p_value varchar2,
    p_calc_dates number,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in imp_run_grid.row_num%type) is

    v_s wp_task_dates.start_date%type;
    v_f wp_task_dates.finish_date%type;
    v_ps wp_tasks.start_projected_date%type;
    v_pf wp_tasks.finish_projected_date%type;
    v_as wp_tasks.start_actual_date%type;
    v_af wp_tasks.finish_actual_date%type;
    v_proms wp_tasks.start_promised_date%type;
    v_promf wp_tasks.finish_promised_date%type;
    v_na wp_tasks.is_not_applicable%type;
    v_dur wp_tasks.duration%type;
    v_window wp_tasks.task_window%type;
    v_percent wp_tasks.percent_complete%type;
    v_comments wp_tasks.comments%type;
    v_flag_id wp_tasks.task_flag_id%type;
    v_doc_name wp_tasks.document_name%type;
    v_calend_id wp_tasks.wp_calendar_id%type;
    v_wbs wp_tasks.wbs%type;
    v_date_type varchar2(20);
    v_errmsg clob;
    v_is_req_task wp_tasks.is_required%type;
    v_date_type_id v_wp_task_date_type.wp_task_date_type_id%type;
    v_uid users.user_id%type;
    v_new_date date;
    v_allow_propagation number;
    begin
      v_date_type := substr(v_date_col, 1, regexp_instr(v_date_col, c_task_order_number_pattern) - 1);
      v_date_type_id := get_date_type_id(p_tid, substr(v_date_type, 1, length(v_date_type) - 1));

      if (v_date_type_id is not null) then
        begin
          select start_date, finish_date, duration, task_window
          into v_s, v_f, v_dur, v_window from wp_task_dates
          where wp_task_id = p_tid and wp_task_date_type_id = v_date_type_id;
          exception
          when no_data_found then
          null;
        end;

        if (substr(v_date_type, length(v_date_type)) = 'S') then
          if (trim(upper(p_value)) = 'NULL') and (pkg_dl_support.AllowNulls = 1) then
            v_s := null;
          else
            v_s := to_date(p_value, v_dtfmt);
          end if;
        else
          if (trim(upper(p_value)) = 'NULL') and (pkg_dl_support.AllowNulls = 1) then
            v_f := null;
          else
            v_f := to_date(p_value, v_dtfmt);
          end if;
        end if;

        select p.user_id into v_uid from process p, imp_run r
        where p.process_id = r.process_id and r.imp_run_id = p_rid;

        pkg_wp.update_date_pair(p_tid, v_date_type_id, v_s, v_f, v_dur, v_window, v_uid);

      elsif p_calc_dates in (1, 2) and v_date_type in ('PS', 'PF', 'AS', 'AF') then
          select start_projected_date, finish_projected_date,
                 start_actual_date, finish_actual_date,
                 start_promised_date, finish_promised_date,
                 duration, task_window, percent_complete,
                 comments, task_flag_id, document_name, wp_calendar_id, wbs
            into v_ps, v_pf,
                 v_as, v_af,
                 v_proms, v_promf,
                 v_dur, v_window, v_percent,
                 v_comments, v_flag_id, v_doc_name, v_calend_id, v_wbs
            from wp_tasks
           where wp_task_id = p_tid;

          if trim(upper(p_value)) = 'NULL' and pkg_dl_support.allownulls = 1 then
              v_new_date := null;
          else
              v_new_date := to_date(p_value, v_dtfmt);
          end if;

          if p_calc_dates = 2 then
              v_allow_propagation := 0;
          else 
              v_allow_propagation := 1;
          end if;

          case v_date_type
              when 'PS' then
                  pkg_wp.update_task(p_tid, v_new_date, v_pf, v_as, v_af, v_allow_propagation);
              when 'PF' then
                  pkg_wp.update_task(p_tid, v_ps, v_new_date, v_as, v_af, v_allow_propagation);
              when 'AS' then
                  pkg_wp.update_task(p_tid, v_ps, v_pf,
                                     v_proms, v_promf, v_new_date, v_af, 0, v_dur, v_window,
                                     v_percent, v_flag_id, v_doc_name, v_comments, v_wbs, v_calend_id, v_allow_propagation);
              when 'AF' then
                  pkg_wp.update_task(p_tid, v_ps, v_pf,
                                     v_proms, v_promf, v_as, v_new_date, 0, v_dur, v_window,
                                     v_percent, v_flag_id, v_doc_name, v_comments, v_wbs, v_calend_id, v_allow_propagation);
          end case;

      elsif (v_date_type = 'NA') and (length(p_value) > 0) then
        select is_required into v_is_req_task from wp_tasks where wp_task_id = p_tid;

        if (v_is_req_task = 1) then
          v_errmsg := 'Task ' || get_task_order_num(v_date_col) || ' is required and can''t be N/A';
          pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_cant_set_na, p_row_num);
        else
          pkg_wp.Update_na(p_tid, pkg_imp_utils.convert_boolean(p_value));
        end if;

      elsif (v_date_type = 'BlockCalc') and (length(p_value) > 0) then
        pkg_wp.Update_Block_calc(p_tid, pkg_imp_utils.convert_boolean(p_value));

      elsif (v_date_type in('PS', 'PF', 'AS', 'AF', 'BS', 'BF', 'RS', 'RF')) then
        if (trim(upper(p_value)) in ('NA','N/A')) then
          set_task_na(p_tid,1);
        elsif ((trim(upper(p_value)) = 'NULL')and(pkg_dl_support.AllowNulls = 1)) then
          pkg_dl_support.set_date(p_tid, null, v_date_col);
        else
          if (v_date_type in ('AS', 'AF')) then
            select is_not_applicable into v_na from wp_tasks where wp_task_id = p_tid;
            if (v_na = 1) then
              update wp_tasks set is_not_applicable = 0 where wp_task_id = p_tid;
            end if;
          end if;
          pkg_dl_support.set_date(p_tid, to_date(p_value, v_dtfmt), v_date_col);
        end if;
      end if;

      exception
      when others then
      v_errmsg := 'Bad Data: "' || p_value || '"' || chr(10) || 'Xitor Key: "'
                  || pkg_imp_run.cell_value(p_rid, p_row_num, 1)
                  || '"' || chr(10) || 'Column: "'
                  || v_date_col || '"' || chr(10) || sqlerrm;
      pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_data, p_row_num);

      raise;
    end set_date_task;


  function get_date_type_id(
    p_tid in wp_tasks.wp_task_id%type,
    p_date_type varchar2)
    return v_wp_task_date_type.wp_task_date_type_id%type
  is
    v_date_type_id v_wp_task_date_type.wp_task_date_type_id%type;
    begin
      select distinct dt.wp_task_date_type_id into v_date_type_id
      from v_wp_task_date_type dt join vw_label l
          on (dt.abbr_app_label_id = l.label_id)
        join wp_template_date_type_xref t_dt_x
          on (t_dt_x.wp_task_date_type_id = dt.wp_task_date_type_id)
        join wp_workplan wp on (wp.template_workplan_id = t_dt_x.wp_workplan_id)
        join wp_tasks t on (t.wp_workplan_id = wp.wp_workplan_id)
      where l.label_text = p_date_type and t.wp_task_id = p_tid
            and dt.is_static <> 1;

      return v_date_type_id;
      exception
      when no_data_found then
      return null;
    end get_date_type_id;

  procedure parse_csv(p_rid in imp_run.imp_run_id%type) is
    r_spec pkg_imp_run.cur_imp_spec%rowtype;
    v_process_id number;
    begin
      select process_id into v_process_id
      from imp_run
      where imp_run_id = p_rid;

      --Search xitor ids
      pkg_imp_run.fill_entity_pks(v_process_id, p_rid);

      --Import columns mapped as for regular config import (non external)
      open pkg_imp_run.cur_imp_spec(p_rid);
      fetch pkg_imp_run.cur_imp_spec into r_spec;
      close pkg_imp_run.cur_imp_spec;

      pkg_imp_run.fill_datamap_sql(r_spec);
      pkg_imp_run.import_data(r_spec);
    end parse_csv;

    /**
     * Local procedure to create/update labels for related languages
     */
    procedure merge_lang_labels(
        p_label_id   in label_program.label_program_id%type,
        p_lang_array in t_lang_array) as
    begin
        for rec_lang in (select label_text, app_lang_id
                           from table(p_lang_array)) loop

            if rec_lang.app_lang_id is not null and rec_lang.label_text is not null then

                pkg_label.set_label_program(
                    p_label_id => p_label_id,
                    p_label    => rec_lang.label_text,
                    p_lang_id  => rec_lang.app_lang_id,
                    p_pid      => pkg_sec.get_pid);
            end if;
        end loop;
    end merge_lang_labels;


    procedure create_avtv (
        p_rid    in imp_run.imp_run_id%type,
        p_avt_id in attrib_v_table.attrib_v_table_id%type,
        p_table_name in attrib_v_table.attrib_v_table_name%type,
        p_order_num  in attrib_v_table_value.order_num%type,      
        p_value in attrib_v_table_value.value%type,
        p_lang_array in t_lang_array,
        p_display    in attrib_v_table_value.display%type,
        p_color      in attrib_v_table_value.color%type)
    as
        v_err_msg clob;
        v_value_label_id label_program.label_program_id%type;
    begin
        begin
            v_value_label_id := pkg_label.create_label_program(p_value);

            insert into attrib_v_table_value (attrib_v_table_id, value, order_num, program_id, value_label_id, display, color)
                                      values (p_avt_id, p_value, p_order_num, pkg_sec.get_pid(), v_value_label_id, nvl(p_display, 1), p_color); -- nvl used here to create new values(if p_display = null) 
                                                                                                                                       -- with display = 1 as it was previously

            merge_lang_labels(v_value_label_id, p_lang_array);
        exception
            when dup_val_on_index  then
                rollback;
                v_err_msg := 'Failed to insert value for table "' || p_table_name || '"->"' || p_value || ', value already exists."' || chr(10) || sqlerrm;
                pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
            when others then
                rollback;
                v_err_msg := 'Failed to insert value for table "' || p_table_name || '"->"' || p_value || '"' || chr(10) || sqlerrm;
                pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
        end;
    end create_avtv;

    procedure avtv_insert_update_attributes(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_attrib_v_table_name in attrib_v_table.attrib_v_table_name%type,
        p_attrib_v_table_id   in attrib_v_table.attrib_v_table_id%type,
        p_order_num in attrib_v_table_value.order_num%type,
        p_value     in attrib_v_table_value.value%type,
        p_imp_action_id in imp_run.imp_action_id%type,
        p_lang_array in t_lang_array,
        p_display    in attrib_v_table_value.display%type,
        p_color      in attrib_v_table_value.color%type)
    as
        v_err_msg clob;
        v_value_label_id attrib_v_table_value.attrib_v_table_value_id%type;
        v_order_num attrib_v_table_value.order_num%type;
        v_display   attrib_v_table_value.display%type;
        v_color     attrib_v_table_value.color%type;
    begin
        select value_label_id, order_num, display, color
          into v_value_label_id, v_order_num, v_display, v_color
          from attrib_v_table_value
         where attrib_v_table_id = p_attrib_v_table_id
           and value = p_value;

        if v_order_num <> p_order_num then
            update attrib_v_table_value
               set order_num = p_order_num
             where attrib_v_table_value_id = v_value_label_id;
        end if;

        if p_display is not null and v_display <> p_display then
            update attrib_v_table_value
               set display = p_display
             where attrib_v_table_value_id = v_value_label_id;
        end if;

        if nvl(v_color, 'no color') <> nvl(p_color, 'no color') then
            update attrib_v_table_value
               set color = p_color
             where attrib_v_table_value_id = v_value_label_id;
        end if;

        merge_lang_labels(v_value_label_id, p_lang_array);
    exception
        when no_data_found then
            if p_imp_action_id = 2 then
                v_err_msg := 'Failed to update attributes for the "' || p_attrib_v_table_name || '"->"' || p_value || '"' || chr(10) || sqlerrm;
                pkg_imp_run.write_error(p_imp_run_id, v_err_msg, pkg_imp_run.c_extimp, 0);    
            elsif p_imp_action_id = 3 then
                create_avtv(p_imp_run_id, p_attrib_v_table_id, p_attrib_v_table_name, p_order_num, p_value, p_lang_array, p_display, p_color);
            end if;                
    end avtv_insert_update_attributes;

    procedure update_avtv(
        p_rid in imp_run.imp_run_id%type,
        p_table_name in attrib_v_table.attrib_v_table_name%type,
        p_val_id     in attrib_v_table_value.attrib_v_table_value_id%type,
        p_order_num  in attrib_v_table_value.order_num%type,
        p_value      in attrib_v_table_value.value%type,
        p_lang_array in t_lang_array,
        p_display    in attrib_v_table_value.display%type,
        p_color      in attrib_v_table_value.color%type)
    as
        v_err_msg clob;
        v_value_label_id label_program.label_program_id%type;
    begin
        update attrib_v_table_value
           set order_num = p_order_num, value = p_value, display = nvl(p_display, display), color = p_color -- nvl used here to prevent error when DISPLAY is not set in the import file
         where attrib_v_table_value_id = p_val_id
         returning value_label_id into v_value_label_id;

        if v_value_label_id is null then
            v_value_label_id := pkg_label.create_label_program(p_value);

			update attrib_v_table_value set value_label_id = v_value_label_id
			 where attrib_v_table_value_id = p_val_id;
        end if;

        merge_lang_labels(v_value_label_id, p_lang_array);       
    exception
        when others then
            rollback;
            v_err_msg := 'Failed to update attributes for the value (' || p_value || ').  Value does not exists for "' || p_table_name || '"->"' || p_value || '"' || chr(10) || sqlerrm;
            pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
    end update_avtv;   


    function check_existing_value_id(
        p_avt_id in attrib_v_table.attrib_v_table_id%type, 
        p_avt_value_id in attrib_v_table_value.attrib_v_table_value_id %type) return boolean
    as
        v_cnt number;
        v_ret boolean;
    begin
        select count(*) into v_cnt
          from attrib_v_table_value
         where attrib_v_table_id = p_avt_id
           and attrib_v_table_value_id = p_avt_value_id;

        if v_cnt = 0 then 
            v_ret := false;
        else
            v_ret := true;
        end if;

        return v_ret;
    end check_existing_value_id; 


    function get_vtable_sql_cursor(p_rid in imp_run.imp_run_id%type) return varchar2 
    as
        v_ret varchar2(32767);
        v_exec_sql_part1 varchar2(4000) := '
            select num.row_num,
                   tab_name.data as table_name,
                   val.data as val,
                   ord_num.data as order_num,
                   coalesce(value_id.data, 0) as value_id,
                   display.data as display,
                   color.data as color';
        v_exec_sql_part2 varchar2(4000);
        v_exec_sql_part3 varchar2(4000):= '
             from (select row_num, data 
                     from imp_run_grid
                    where imp_run_id = ' || p_rid || '
                      and col_num = 1) num
             left outer join (select row_num, data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''TABLE_NAME'')
                             ) tab_name on (num.row_num = tab_name.row_num)
             left outer join (select row_num, data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''VALUE'')
                             ) val on (num.row_num = val.row_num)
             left outer join (select row_num, data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and (col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''ORDER_NUMBER'') or col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''ORDER_NUM''))
                             ) ord_num on (num.row_num = ord_num.row_num)
             left outer join (select row_num, to_number(data) as data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''ATTRIB_V_TABLE_VALUE_ID'')
                             ) value_id on (num.row_num = value_id.row_num)
             left outer join (select row_num, case when data is not null then pkg_imp_utils.convert_boolean(data) else null end as data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''DISPLAY'')
                             ) display on (num.row_num = display.row_num)
             left outer join (select row_num, data from imp_run_grid
                               where imp_run_id = ' || p_rid || ' and data is not null
                                 and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''COLOR'')
                             ) color on (num.row_num = color.row_num)';
        v_exec_sql_part4 varchar2(4000);
        v_exec_sql_part5 varchar2(255) := 'where num.row_num > (select start_row
                                                                  from imp_run
                                                                 where imp_run_id = ' || p_rid || ')';
    begin
        --prepare part of the sql using languages
        for rec_lang in (select app_lang_id, 
                                lang, max_lang.rn
                           from (select rownum as rn from dual connect by rownum <= 5) max_lang
                           left join (select app_lang_id,
                                             upper(app_lang_description) as lang,
                                             rownum as rn
                                        from app_languages al 
                                       where app_lang_id <> 98) al on al.rn = max_lang.rn) loop

            --add sql part(join and column) if app_lang_id is not null
            if rec_lang.app_lang_id is not null then
                v_exec_sql_part2 := v_exec_sql_part2 || ', lang' || rec_lang.rn || '.label_text_lang' || rec_lang.rn || ', lang' || rec_lang.rn || '.app_lang_id' || rec_lang.rn || pkg_str.c_lb;

                v_exec_sql_part4 := v_exec_sql_part4 || '
                    left outer join (select row_num, 
                                            data as label_text_lang' || rec_lang.rn || ',
                                            ' || rec_lang.app_lang_id || ' as app_lang_id' || rec_lang.rn || '
                                       from imp_run_grid
                                      where imp_run_id = ' || p_rid || ' and data is not null
                                        and col_num = pkg_imp_utils.get_col_num(' || p_rid || ', ''' || rec_lang.lang || ''')
                                    ) lang' || rec_lang.rn || ' on (num.row_num = lang' || rec_lang.rn || '.row_num)' || pkg_str.c_lb;
            else
                v_exec_sql_part2 := v_exec_sql_part2 || ', null as label_text_lang' || rec_lang.rn || ', null as app_lang' || rec_lang.rn || pkg_str.c_lb;
            end if;
        end loop;

        v_ret := v_exec_sql_part1 || pkg_str.c_lb || v_exec_sql_part2 || v_exec_sql_part3 || v_exec_sql_part4 || v_exec_sql_part5;

        return v_ret;
    end get_vtable_sql_cursor;


    procedure ConfiguredVTableLoad(p_rid in imp_run.imp_run_id%type) is
        v_pid number;
        v_vtab_id number;
        v_err_msg clob;
        v_imp_action_id number;
        v_rowcount number := 0;
        v_exec_sql varchar2(32767);

        type t_refcursor is ref cursor;
        v_cur_imp_data t_refcursor;
        v_cur_rowcount number;

        type t_imp_rec is record(
            row_num     imp_run_grid_incr.row_num%type,
            table_name  imp_run_grid_incr.data%type,
            val         imp_run_grid_incr.data%type,
            order_num   attrib_v_table_value.order_num%type,
            value_id    attrib_v_table_value.attrib_v_table_value_id%type,
            display     attrib_v_table_value.display%type,
            color       attrib_v_table_value.color%type,
            label_text_lang1 label_program.label_program_text%type,
            app_lang_id1     app_languages.app_lang_id%type,
            label_text_lang2 label_program.label_program_text%type,
            app_lang_id2     app_languages.app_lang_id%type,
            label_text_lang3 label_program.label_program_text%type,
            app_lang_id3     app_languages.app_lang_id%type,
            label_text_lang4 label_program.label_program_text%type,
            app_lang_id4     app_languages.app_lang_id%type,
            label_text_lang5 label_program.label_program_text%type,
            app_lang_id5     app_languages.app_lang_id%type);

        v_imp_rec t_imp_rec;
        v_lang_array t_lang_array;
    begin
        v_exec_sql := get_vtable_sql_cursor(p_rid);

        select program_id, imp_action_id into v_pid, v_imp_action_id
          from imp_run
         where imp_run_id = p_rid;

        open v_cur_imp_data for v_exec_sql;
            loop
                v_cur_rowcount := v_cur_imp_data%rowcount;
                    fetch v_cur_imp_data into v_imp_rec;
                    exit when v_cur_imp_data%notfound;

                    v_vtab_id := null;

                    begin
                        select attrib_v_table_id into v_vtab_id
                          from attrib_v_table
                         where attrib_v_table_name = v_imp_rec.table_name
                           and program_id = v_pid;
                    exception
                        when no_data_found then
                            begin
                                insert into attrib_v_table(attrib_v_table_name, program_id)
                                     values (v_imp_rec.table_name, v_pid)
                                  returning attrib_v_table_id into v_vtab_id;
                            exception
                                when others then
                                    v_err_msg := 'Failed to create VTable "' || v_imp_rec.table_name || chr(10) || sqlerrm;
                                    pkg_imp_run.write_error(p_rid, v_err_msg, pkg_imp_run.c_extimp, 0);
                            end;
                    end;

                    v_lang_array := t_lang_array(); --initialize new empty collection
                    v_lang_array.extend;
                    v_lang_array(v_lang_array.last).label_text  := v_imp_rec.label_text_lang1;
                    v_lang_array(v_lang_array.last).app_lang_id := v_imp_rec.app_lang_id1;
                    v_lang_array.extend;
                    v_lang_array(v_lang_array.last).label_text  := v_imp_rec.label_text_lang2;
                    v_lang_array(v_lang_array.last).app_lang_id := v_imp_rec.app_lang_id2;
                    v_lang_array.extend;
                    v_lang_array(v_lang_array.last).label_text  := v_imp_rec.label_text_lang3;
                    v_lang_array(v_lang_array.last).app_lang_id := v_imp_rec.app_lang_id3;
                    v_lang_array.extend;
                    v_lang_array(v_lang_array.last).label_text  := v_imp_rec.label_text_lang4;
                    v_lang_array(v_lang_array.last).app_lang_id := v_imp_rec.app_lang_id4;
                    v_lang_array.extend;
                    v_lang_array(v_lang_array.last).label_text  := v_imp_rec.label_text_lang5;
                    v_lang_array(v_lang_array.last).app_lang_id := v_imp_rec.app_lang_id5;

                    if v_imp_rec.value_id = 0 and v_vtab_id is not null then
                        if v_imp_action_id in(2, 3) then --upd, ins/upd
                            avtv_insert_update_attributes(p_rid, v_imp_rec.table_name, v_vtab_id, v_imp_rec.order_num, v_imp_rec.val, v_imp_action_id, v_lang_array, v_imp_rec.display, v_imp_rec.color);      

                        elsif v_imp_action_id = 4 then --ins
                            create_avtv(p_rid, v_vtab_id, v_imp_rec.table_name, v_imp_rec.order_num, v_imp_rec.val, v_lang_array, v_imp_rec.display, v_imp_rec.color);
                        end if;

                    elsif v_imp_rec.value_id <> 0 and v_vtab_id is not null then
                        if v_imp_action_id = 2 then --upd
                            update_avtv(p_rid, v_imp_rec.table_name, v_imp_rec.value_id, v_imp_rec.order_num, v_imp_rec.val, v_lang_array, v_imp_rec.display, v_imp_rec.color);

                        elsif v_imp_action_id = 3 then --ins/upd
                            if check_existing_value_id(v_vtab_id, v_imp_rec.value_id) then   
                                update_avtv(p_rid, v_imp_rec.table_name, v_imp_rec.value_id, v_imp_rec.order_num, v_imp_rec.val, v_lang_array, v_imp_rec.display, v_imp_rec.color);
                            else    
                                create_avtv(p_rid, v_vtab_id, v_imp_rec.table_name, v_imp_rec.order_num, v_imp_rec.val, v_lang_array, v_imp_rec.display, v_imp_rec.color);
                            end if;

                        elsif v_imp_action_id = 4 then --ins
                            create_avtv(p_rid, v_vtab_id, v_imp_rec.table_name, v_imp_rec.order_num, v_imp_rec.val, v_lang_array, v_imp_rec.display, v_imp_rec.color);
                        end if;

                    end if;    

                    v_rowcount := v_rowcount + 1;
                    pkg_imp_run.set_rows(p_rid, v_rowcount);
                    commit;
            end loop;
        close v_cur_imp_data;
    exception
        when others then 
            close v_cur_imp_data;
            raise_application_error(-20000, dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace);
    end ConfiguredVTableLoad;

  procedure CfTaskDateExcelSubmit(p_rid in imp_run.imp_run_id%type)
  as
    c_dt_start_pref varchar2(10) := 'START_td';
    c_dt_finish_pref varchar2(10) := 'FINISH_td';
    c_dt_sproj varchar2(10) := c_dt_start_pref || '1';
    c_dt_sprom varchar2(10) := c_dt_start_pref || '2';
    c_dt_sact varchar2(10) := c_dt_start_pref || '3';
    c_dt_fproj varchar2(10) := c_dt_finish_pref || '1';
    c_dt_fprom varchar2(10) := c_dt_finish_pref || '2';
    c_dt_fact varchar2(10) := c_dt_finish_pref || '3';

    cursor cur_data(
        p_rid in imp_run.imp_run_id%type,
        p_start_row in imp_run.start_row%type)
    is
      select irg_data.row_num as row_num, irg_pk.data as pk, irg_id.data as id, irg_data.data as val
        from imp_run_grid irg_pk,
            (select row_num, col_num, data from imp_run_grid
              where imp_run_id = p_rid and row_num = p_start_row and col_num > 1) irg_id,
            (select row_num, col_num, data from imp_run_grid
              where imp_run_id = p_rid and row_num > p_start_row and col_num > 1) irg_data
       where irg_pk.imp_run_id = p_rid and irg_pk.row_num > p_start_row and irg_pk.col_num = 1
         and irg_data.row_num = irg_pk.row_num and irg_id.col_num = irg_data.col_num
       order by irg_data.row_num asc;

    v_row_num imp_run_grid.row_num%type;
    v_pk number;
    v_sproj date;
    v_fproj date;
    v_sprom date;
    v_fprom date;
    v_sact date;
    v_fact date;
    v_sdyn date;
    v_fdyn date;
    v_date_type number;
    v_dur number;
    v_taskwin number;
    v_start_row imp_run.start_row%type;
  begin
      v_row_num := 0;
      pkg_imp_run.drop_pks(p_rid);

      select start_row
        into v_start_row
        from imp_run
       where imp_run_id = p_rid;

      for rec_data in cur_data(p_rid, v_start_row) loop
          begin
              v_pk := to_number(rec_data.pk);

              if (rec_data.val != '$NA%' and pkg_str.is_number(rec_data.id)) then
                  pkg_dl_support.set_cf_data(to_number(rec_data.id), v_pk, to_clob(rec_data.val));
              elsif (rec_data.val != '$NA%' and not pkg_str.is_number(rec_data.id)) then
                  if (rec_data.id = c_dt_sproj or rec_data.id = c_dt_sprom or rec_data.id = c_dt_sact
                      or rec_data.id = c_dt_fproj or rec_data.id = c_dt_fprom or rec_data.id = c_dt_fact) then
                      select start_projected_date, finish_projected_date,
                             start_promised_date, finish_promised_date,
                             start_actual_date, finish_actual_date
                        into v_sproj, v_fproj, v_sprom, v_fprom, v_sact, v_fact
                        from wp_tasks where wp_task_id = v_pk;

                      if (rec_data.id = c_dt_sproj) then
                          v_sproj := to_date(rec_data.val);
                      elsif (rec_data.id = c_dt_sprom) then
                          v_sprom := to_date(rec_data.val);
                      elsif (rec_data.id = c_dt_sact) then
                          v_sact := to_date(rec_data.val);
                      elsif (rec_data.id = c_dt_fproj) then
                          v_fproj := to_date(rec_data.val);
                      elsif (rec_data.id = c_dt_fprom) then
                          v_fprom := to_date(rec_data.val);
                      elsif (rec_data.id = c_dt_fact) then
                          v_fact := to_date(rec_data.val);
                      end if;

                      pkg_wp.update_task(v_pk, v_sproj, v_fproj, v_sprom, v_fprom, v_sact, v_fact);
                  else
                      if (instr(rec_data.id, c_dt_start_pref) != 0) then
                          v_date_type := to_number(replace(rec_data.id, c_dt_start_pref));
                      elsif (instr(rec_data.id, c_dt_finish_pref) != 0) then
                          v_date_type := to_number(replace(rec_data.id, c_dt_finish_pref));
                      end if;

                      select start_date, finish_date, duration, task_window
                        into v_sdyn, v_fdyn, v_dur, v_taskwin
                        from wp_task_dates where wp_task_id = v_pk and wp_task_date_type_id = v_date_type;

                      if (instr(rec_data.id, c_dt_start_pref) != 0) then
                          v_sdyn := to_date(rec_data.val);
                      elsif (instr(rec_data.id, c_dt_finish_pref) != 0) then
                          v_fdyn := to_date(rec_data.val);
                      end if;

                      pkg_wp.update_date_pair(v_pk, v_date_type, v_sdyn, v_fdyn, v_dur, v_taskwin, null);
                  end if;
              end if;

          exception when others then
               pkg_imp_run.write_error(p_rid, sqlerrm, pkg_imp_run.c_et_data, rec_data.row_num);
          end;

          if (v_row_num != rec_data.row_num) then
              pkg_imp_run.add_pk(p_rid, null, rec_data.row_num, rec_data.pk, 0);
              v_row_num := rec_data.row_num;
          end if;
      end loop;
  end CfTaskDateExcelSubmit;

  procedure ConfiguredFields(p_rid in imp_run.imp_run_id%type)
  is
    begin
      pkg_ext_imp_field.ConfiguredFields(p_rid);
    end ConfiguredFields;

    procedure ConfiguredFieldsWithTabs(p_rid in imp_run.imp_run_id%type)
    is
    begin
        pkg_ext_imp_field.ConfiguredFieldsWithTabs(p_rid);
    end ConfiguredFieldsWithTabs;

   /**
    * Local procedure to load task, system and program labels
    *
    * @param p_rid Import run id
    * @param p_imp_type Import type (constant of: c_label_system_imp, c_label_program_imp, c_label_task_imp)
    */
    procedure label_import(p_rid in imp_run.imp_run_id%type, p_imp_type in number)
    as
        v_rowcount number := 1;
        v_errmsg clob;

        cursor cur_labels_imp_languages(p_rid in imp_run.imp_run_id%type) is
            select l.app_lang_id, 
                   irg.col_num as label_text_col_num,
                   rownum as rn
              from app_languages l
              join imp_run_grid irg on irg.data = l.app_lang_description
             where irg.row_num = (select start_row
                                    from imp_run
                                   where imp_run_id = p_rid) 
               and l.is_hidden = 0
               and irg.imp_run_id = p_rid;

        cursor cur_labels_imp_run_grid(
            p_rid in imp_run.imp_run_id%type, 
            p_label_text_col_num in varchar2)
        is
            select to_number(label_id.data) as label_id, 
                   label_text.data as label_text,
                   label_id.row_num
              from (select row_num, nvl(clob_data, data) as data 
                      from imp_run_grid
                     where imp_run_id = p_rid
                       and col_num = 1
                   ) label_id,
                   (select row_num, nvl(clob_data, data) as data
                      from imp_run_grid
                     where imp_run_id = p_rid
                       and col_num = p_label_text_col_num
                   ) label_text
             where label_id.row_num = label_text.row_num
               and label_id.row_num > (select start_row
                                         from imp_run
                                        where imp_run_id = p_rid);
    begin
        for rec_lang in cur_labels_imp_languages(p_rid) loop
            for rec_label in cur_labels_imp_run_grid(p_rid, rec_lang.label_text_col_num) loop
                begin
                    if rec_label.label_text is not null then
                        case p_imp_type
                            when c_imp_label_system then 
                                pkg_label.update_label_system(
                                    p_label_id   => rec_label.label_id,
                                    p_label_text => rec_label.label_text,
                                    p_lang_id    => rec_lang.app_lang_id);
                            when c_imp_label_program then
                                pkg_label.update_label_program(
                                    p_label_id   => rec_label.label_id,
                                    p_label_text => rec_label.label_text,
                                    p_lang_id    => rec_lang.app_lang_id);
                            when c_imp_label_task then
                                pkg_label.update_label_task(
                                    p_label_id   => rec_label.label_id,
                                    p_label_text => rec_label.label_text,
                                    p_lang_id    => rec_lang.app_lang_id);
                        end case;
                    end if;

                    if rec_lang.rn = 1 then
                        v_rowcount := v_rowcount + 1;
                        pkg_imp_run.set_rows(p_rid, v_rowcount);
                    end if;

                    commit;
                exception
                    when others then 
                         v_errmsg := 'Unknown error. Label id = ' || rec_label.label_id || '. Error stack: ' || dbms_utility.format_error_stack;
                         pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_data, rec_label.row_num);
                end;
            end loop;
        end loop;
    exception
        when others then 
            v_errmsg := dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace;
            pkg_imp_run.write_error(p_rid, v_errmsg, pkg_imp_run.c_et_unknown, null);
    end label_import;


    procedure label_program_load(p_rid in imp_run.imp_run_id%type) as
    begin
        label_import(p_rid, c_imp_label_program);
    end label_program_load;


    procedure label_system_load(p_rid in imp_run.imp_run_id%type) as
    begin
        label_import(p_rid, c_imp_label_system);
    end label_system_load;


    procedure label_task_load(p_rid in imp_run.imp_run_id%type) as
    begin
        label_import(p_rid, c_imp_label_task);
    end label_task_load;
end pkg_ext_imp;
/