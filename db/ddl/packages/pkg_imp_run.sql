CREATE OR REPLACE PACKAGE BODY PKG_IMP_RUN 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

  c_csv_err_header constant varchar2(100) := 'ROW_NUM,ERROR_MSG,IMP_ERROR_TYPE';
  c_lb constant varchar2(2) := chr(13) || chr(10);

  csv_delims pkg_imp_run.t_delimiters;
  quote_rules pkg_imp_run.t_quote_rules;

  /**
   * Utility procedure. Executes common functions to prepare data for import.
   */
  procedure prepare_import(p_rid in imp_run.imp_run_id%type);

  /**
   * Fill csv_delims and quote_rules global variables
   */
  procedure set_delimiters(p_rid in imp_run.imp_run_id%type);

  procedure search_field_info(p_xtid in xitor_type.xitor_type_id%type,
                              p_name in config_field.config_field_name%type,
                              p_fld_id out config_field.config_field_id%type,
                              p_static out config_field.is_static%type,
                              p_type out config_field.data_type%type);

  /**
   * Utility procedure to execute rules. It can execute 18,19 and 31 rule types.
   * Will fully rolback xitor creation and clear audit_log (for rule type 18)
   * and rule updated for all supported rule types
   */
  procedure exec_trigger(
    p_rtid in rule.rule_type_id%type,
    p_xtid in xitor_type.xitor_type_id%type,
    p_pk in xitor.xitor_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in number);

  procedure get_csv_row(
    p_rid in imp_run_error.imp_run_id%type,
    p_row_num in imp_run_error.row_num%type, p_row in out nocopy clob);

  /**
   * Static implementation of "Configurable Field" imp data type
   */
  procedure imp_cf(
    p_cfid in config_field.config_field_id%type,
    p_add_vtable_value in number,
    p_force_update in number,
    p_ent_pk imp_run_entity_pk.pk%type,
    p_val in out nocopy clob,
    p_date_format in imp_spec.date_format%type,
    p_time_format in imp_spec.time_format%type,
    p_pid in program.program_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in number,
    p_dmid in imp_data_map.imp_data_map_id%type);

  /**
   * Static implementation of "Relation" and "Relation by xitor_id"
   * imp data types. Use p_parent_id param for "Relation by xitor_id",
   * p_val for "Relation"
   */
  procedure create_rel(
    p_parent_xtid in xitor_type.xitor_type_id%type,
    p_del_existing_rel in varchar2,
    p_ent_pk imp_run_entity_pk.pk%type,
    p_val in xitor.xitor_key%type,
    p_pid in program.program_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in number,
    p_dmid in imp_data_map.imp_data_map_id%type,
    p_parent_id in xitor.xitor_id%type default null);

  /**
   * Static implementation of "EFile" imp data type
   */
  procedure imp_efile(
    p_cfid in config_field.config_field_id%type,
    p_pk imp_run_entity_pk.pk%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in integer,
    p_col_num in integer,
    p_dmid in imp_run_error.imp_data_map_id%type,
    p_start_row in imp_run.start_row%type default 1);

  function fill_grid(
    rid imp_run.imp_run_id%type,
    p_update_imp_status in boolean default true)
    return number;

  function escape_csv_value(
    value in varchar2,
    delimiters in t_delimiters,
    quote_rules in t_quote_rules,
    quote_value in boolean default false)
    return varchar2;

  procedure no_direct_import_mods as
    begin
      if not allow_import_mods then
        raise_application_error (-20000, pkg_label.get_label_system(17684));
      end if;
    end no_direct_import_mods;

  function delete_import(p_imp_spec_id imp_spec.imp_spec_id%type) return number
  as
    v_import imp_spec%rowtype;
    v_cnt number;
    v_deleted_rows number;
    begin
      allow_import_mods := true;

      select * into v_import
      from imp_spec
      where imp_spec_id = p_imp_spec_id;

      if not gv_is_dropgrid_deleting then
        select count(imp_run_id) into v_cnt
        from imp_run ir join process p on (p.process_id = ir.process_id)
        where imp_spec_id = v_import.imp_spec_id
              and p.status_id in (0,1,4,5,8,9,10,11,13);

        if v_cnt > 0 then
          raise_application_error (-20000, pkg_label.get_label_system(17685));
        end if;
      end if;

      delete from process where process_id in (select process_id from imp_run ir
      where ir.imp_spec_id=v_import.imp_spec_id);

      update trackor_form set imp_spec_id = null
      where imp_spec_id = v_import.imp_spec_id;

      delete from rule_id_num
      where id_num = p_imp_spec_id
        and rule_id in (select rule_id from rule
                         where rule_type_id in (pkg_ruleator.c_type_import_completed,
                                                pkg_ruleator.c_type_import_started));

      delete from blob_data 
       where blob_data_id in (select template_blob_data_id
                                from imp_spec
                               where template_blob_data_id is not null
                                 and imp_spec_id = v_import.imp_spec_id);

      delete from imp_spec where imp_spec_id = v_import.imp_spec_id;
      v_deleted_rows := sql%rowcount;

      allow_import_mods := false;

      return v_deleted_rows;
    end delete_import;

  procedure delete_import(p_imp_spec_id imp_spec.imp_spec_id%type)
  as
    v_ignored number;
    begin
      v_ignored := delete_import(p_imp_spec_id);
    end delete_import;

  procedure write_error(
    p_rid in imp_run_error.imp_run_id%type,
    p_msg in clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_sql in imp_run_error.sql_text%type default null,
    p_dmid in imp_run_error.imp_data_map_id%type default null,
    p_entity_id in imp_run_error.entity_id%type default null,
    p_col_name in imp_run_error.col_name%type default null,
    p_bad_data_value in imp_run_error.bad_data_value%type default null) as
  pragma autonomous_transaction;

    begin
      insert into imp_run_error(imp_run_id, error_msg, sql_text,
                                imp_error_type_id, row_num, imp_data_map_id,
                                entity_id, col_name, bad_data_value)
      values (p_rid, dbms_lob.substr(p_msg, 2000, 1), p_sql, p_err_type_id, p_row_num, 
              p_dmid, p_entity_id, p_col_name, p_bad_data_value);
      commit;
    end write_error;

  /**
   * Convert 1-based column number to Excel col name
   */
  function get_excell_col_name(p_col_num in number) return varchar2 is
    v_dividend integer;
    v_modulo integer;
    v_col_name varchar2(30);
    begin
      v_dividend := p_col_num;

      while (v_dividend > 0) loop
        v_modulo := mod(v_dividend - 1, 26);
        v_col_name := concat(chr(65 + v_modulo), v_col_name);
        v_dividend := (v_dividend - v_modulo) / 26;
      end loop;

      return v_col_name;
    end get_excell_col_name;


  procedure write_field_comment(
      p_rid in imp_run_error.imp_run_id%type,
      p_msg in field_comment.comments%type,
      p_row_num in imp_run_error.row_num%type,
      p_field_name in field_comment.field_name%type,
      p_pk in field_comment.pk%type,
      p_ttid in field_comment.trackor_type_id%type,
      p_dmid in imp_run_error.imp_data_map_id%type,
      p_val in out nocopy clob,
      p_imp_col_num number default null,
      p_log_error number default 0)
  as
      pragma autonomous_transaction;
      v_imp_column_id imp_data_map.imp_column_id%type;
      v_comment_msg field_comment.comments%type;
      v_process_id imp_run.process_id%type;
      v_col_num number;
      v_log_warns imp_spec.log_warnings_in_comments%type;
  begin
      select r.process_id, s.log_warnings_in_comments
        into v_process_id, v_log_warns
        from imp_run r join imp_spec s on (s.imp_spec_id = r.imp_spec_id)
       where r.imp_run_id = p_rid;

      if not(v_log_warns <> 1 and p_log_error = 0) then
          begin
              select imp_column_id into v_imp_column_id
                from imp_data_map where imp_data_map_id = p_dmid;

              v_col_num := col_num(p_rid, v_imp_column_id); -- get column number from imp_data_map.imp_column_id
          exception
              when no_data_found then
                  v_col_num := p_imp_col_num; --p_imp_col_num - column number from imported file
          end;

          v_comment_msg := 'Data import error. Process ID: "' || v_process_id || '"' || c_lb
                           || 'Error message: ' || c_lb
                           || p_msg || c_lb || c_lb
                           || 'Cell: ' || get_excell_col_name(v_col_num) || p_row_num || c_lb;

          insert into field_comment (field_name, pk, comments, trackor_type_id)
          values (p_field_name, p_pk, v_comment_msg, p_ttid);
          commit;
      end if;
  end write_field_comment;

    procedure get_csv_row(
        p_rid in imp_run_error.imp_run_id%type,
        p_row_num in imp_run_error.row_num%type,
        p_row in out nocopy clob) as
    begin
        dbms_lob.createtemporary(p_row, true);

        for rec in (select data
                      from imp_run_grid 
                     where imp_run_id = p_rid
                       and row_num = p_row_num
                     order by col_num) loop
            dbms_lob.append(p_row, escape_csv_value(rec.data, csv_delims, quote_rules) || csv_delims.fields_delimiter);
        end loop;

        if dbms_lob.getlength(p_row) > 0 then
            dbms_lob.trim(p_row, dbms_lob.getlength(p_row) - length(csv_delims.fields_delimiter));
        end if;
    end get_csv_row;

  function run_sql(
    p_rid in imp_run_error.imp_run_id%type,
    p_sql in out nocopy clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_dmid in imp_run_error.imp_data_map_id%type default null)
    return number as

  pragma autonomous_transaction;

    errmsg clob;
    ssql2 clob;
    begin
      ssql2:= replace(p_sql, chr(13), '');
      ssql2:= replace(ssql2, chr(10), ' ');
      execute immediate cast(ssql2 as long);
      commit;
      return 1;
      exception
      when others then
      errmsg := dbms_utility.format_error_stack;
      write_error(p_rid, errmsg, p_err_type_id, p_row_num, p_sql, p_dmid);
      return 0;
    end run_sql;

  procedure run_sql(
    p_rid in imp_run_error.imp_run_id%type,
    p_sql in out nocopy clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_dmid in imp_run_error.imp_data_map_id%type default null) as

    v_value number;
    begin
      v_value := run_sql(p_rid, p_sql, p_err_type_id, p_row_num, p_dmid);
    end run_sql;

  function run_sql_ret(
    p_rid in imp_run_error.imp_run_id%type,
    p_sql in out nocopy clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_dmid in imp_run_error.imp_data_map_id%type default null) return number as

  pragma autonomous_transaction;

    v_value number;
    errmsg clob;
    ssql2 clob;
    begin
      ssql2:= replace(p_sql, chr(13), '');
      ssql2:= replace(ssql2, chr(10), ' ');

      execute immediate cast(ssql2 as long) using out v_value;
      commit;
      return v_value;
      exception
      when others then
      errmsg := dbms_utility.format_error_stack;
      write_error(p_rid, errmsg, p_err_type_id, p_row_num, p_sql, p_dmid);
      return null;
    end run_sql_ret;

  function run_sql_ret_char(
    p_rid in imp_run_error.imp_run_id%type,
    p_sql in out nocopy clob,
    p_err_type_id in imp_run_error.imp_error_type_id%type,
    p_row_num in imp_run_error.row_num%type,
    p_dmid in imp_run_error.imp_data_map_id%type default null) return clob as

  pragma autonomous_transaction;

    v_value clob;
    v_err_code number;
    v_err_msg clob;
    ssql2 clob;
    begin
      ssql2:= replace(p_sql, chr(13), '');
      ssql2:= replace(ssql2, chr(10), ' ');

      execute immediate cast(ssql2 as long) using out v_value;
      commit;
      return v_value;
      exception
      when others then
      v_err_code := sqlcode;
      v_err_msg := sqlerrm;

      if (v_err_code = 1 and v_err_msg = 'User-Defined Exception') then
        --seems this is e_skip_row exception, skip this row
        raise e_skip_row;
      end if;

      v_err_msg := dbms_utility.format_error_stack;
      write_error(p_rid, v_err_msg, p_err_type_id, p_row_num, p_sql, p_dmid);
      return null;
    end run_sql_ret_char;


  procedure set_rows(rid number, num number) as
  pragma autonomous_transaction;
    begin
      update imp_run set rows_processed = num where imp_run_id = rid;
      commit;
    end;

  procedure set_status(
    p_proc_id process.process_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_status_id process.status_id%type) as
  pragma autonomous_transaction;
    v_err_count number;
    begin
      if (p_status_id = c_s_end) then
        select count(*) into v_err_count from imp_run_error
        where imp_run_id = p_rid;

        if (v_err_count > 0) then
          update process set end_date = current_date, status_id = c_s_end_errs
          where process_id = p_proc_id;
        else
          update process set end_date = current_date, status_id = c_s_end
          where process_id = p_proc_id;
        end if;
      elsif (p_status_id = c_s_interrupted) then
        update process set end_date = current_date, status_id = c_s_interrupted
        where process_id = p_proc_id;
      elsif (p_status_id = c_s_end_errs) then
        update process set end_date = current_date, status_id = c_s_end_errs
        where process_id = p_proc_id;
      else
        update process set status_id = p_status_id where process_id = p_proc_id;
      end if;

      commit;
    end set_status;

  function ireplace(srcstr clob, oldsub clob, newsub clob) return clob 
  is
      brk binary_integer;
      v_ret clob;
  begin
      if srcstr is null then
          v_ret := srcstr;
      else
          brk := instr(upper(srcstr), upper(oldsub));
          if brk > 0 then
              v_ret := substr(srcstr, 1, brk - 1) || newsub || ireplace(substr(srcstr, brk + length(oldsub)), oldsub, newsub);
          else
              v_ret := srcstr;
          end if;
      end if;

      return v_ret;
  end ireplace;

  procedure set_cell(
    p_rid in imp_run.imp_run_id%type,
    p_rn in imp_run_grid.row_num%type,
    p_cn in imp_run_grid.col_num%type,
    p_data in out nocopy clob,
    p_quote in varchar2)
  as
  pragma autonomous_transaction;
    v_tmp_data clob;
    v_amount integer;
    begin
      dbms_lob.createtemporary(v_tmp_data, true);
      if dbms_lob.instr(p_data, p_quote) = 1 then
        v_amount := dbms_lob.getlength(p_data) - (2*length(p_quote));
        if (v_amount > 0) then
          dbms_lob.copy(v_tmp_data, p_data, v_amount, 1, length(p_quote) + 1);
        end if;
        p_data := v_tmp_data;
      end if;

      if (dbms_lob.substr(p_data, 2, 1) = '''0') then
        dbms_lob.copy(v_tmp_data, p_data, dbms_lob.getlength(p_data), 1, 2);
        p_data := v_tmp_data;
      end if;

      p_data := replace (p_data, p_quote || p_quote, p_quote);
      p_data := replace (p_data,'''','''''');
      p_data := replace (p_data, chr(13));
      p_data := replace (p_data, chr(0));

      if (dbms_lob.getlength(p_data) > 2000) then
        insert into imp_run_grid_incr (imp_run_id,row_num,col_num,clob_data,is_delta_val)
        values(p_rid, p_rn, p_cn, p_data, 0);
      else
        insert into imp_run_grid_incr (imp_run_id,row_num,col_num,data,is_delta_val)
        values(p_rid, p_rn, p_cn, to_char(p_data), 0);
      end if;
      commit;
    end set_cell;

  function csv_to_cells(
    p_rid in imp_run.imp_run_id%type,
    p_data in out nocopy clob,
    p_line_delim in varchar2,
    p_fld_delim  in varchar2,
    p_quote  in varchar2) return number
  is
    v_pos_line_delim integer;
    v_pos_qoute_str integer;
    v_pos_fld_delim integer;
    v_pos_start_cell integer := 1;
    v_row_num pls_integer := 0;
    v_col_num pls_integer := 1;
    v_quoted_val boolean := false;
    v_cell_data clob;
    v_cell_data2 clob;
    v_amount integer;
    v_len integer;
    v_exit boolean := false;
    v_len_line_delim integer;
    v_len_fld_delim integer;
    v_len_qoute_str integer;

    function l_instr(p_data in out nocopy clob,
                     p_pattern in varchar2,
                     p_offset in integer,
                     p_default in integer) return integer
    is
      v_pos integer;
      begin
        v_pos := dbms_lob.instr(p_data, p_pattern, p_offset);
        if (v_pos = 0) then --pattern not found
          v_pos := p_default;
        end if;
        return v_pos;
      end;

    begin
      v_len := dbms_lob.getlength(p_data) + 1;
      v_len_line_delim := length(p_line_delim);
      v_len_fld_delim := length(p_fld_delim);
      v_len_qoute_str := length(p_quote);

      v_pos_line_delim := l_instr(p_data, p_line_delim, 1, v_len);
      v_pos_qoute_str := l_instr(p_data, p_quote, 1, v_len);
      v_pos_fld_delim := l_instr(p_data, p_fld_delim, 1, v_len);
      dbms_lob.createtemporary(v_cell_data, true);
      loop
        if (v_pos_qoute_str < v_pos_line_delim) and (v_pos_qoute_str < v_pos_fld_delim) then
          --next token is quote string
          v_quoted_val := not v_quoted_val;
          v_pos_qoute_str := l_instr(p_data, p_quote, v_pos_qoute_str + 1, v_len);

        elsif (v_pos_line_delim < v_pos_qoute_str)
              and ((v_pos_line_delim < v_pos_fld_delim) or (v_pos_fld_delim = v_len))
              and (v_pos_fld_delim <= v_len) then
          -- next token is new line
          if v_quoted_val then
            v_pos_line_delim := l_instr(p_data, p_line_delim, v_pos_line_delim + 1, v_len);
          else
            v_amount := v_pos_line_delim - v_pos_start_cell;
            if (v_amount > 0) then --not empty cell
              dbms_lob.copy(v_cell_data, p_data, v_amount, 1, v_pos_start_cell);
            end if;
            set_cell(p_rid, v_row_num, v_col_num, v_cell_data, p_quote);
            v_row_num := v_row_num + 1;
            v_col_num := 1;

            v_pos_start_cell := v_pos_line_delim + v_len_line_delim;

            v_pos_line_delim := l_instr(p_data, p_line_delim, v_pos_start_cell, v_len);
            v_pos_qoute_str := l_instr(p_data, p_quote, v_pos_start_cell, v_len);
            v_pos_fld_delim := l_instr(p_data, p_fld_delim, v_pos_start_cell, v_len);
          end if;

        elsif (v_pos_fld_delim < v_pos_qoute_str) and (v_pos_fld_delim < v_pos_line_delim) then
          -- next token is separator
          if not v_quoted_val then
            v_amount := v_pos_fld_delim - v_pos_start_cell;
            if (v_amount > 0) then --not empty cell
              dbms_lob.copy(v_cell_data, p_data, v_amount, 1, v_pos_start_cell);
            end if;
            set_cell(p_rid, v_row_num, v_col_num, v_cell_data, p_quote);

            v_col_num := v_col_num + 1;

            v_pos_start_cell := v_pos_fld_delim + v_len_fld_delim;
          end if;
          v_pos_fld_delim := l_instr(p_data, p_fld_delim, v_pos_fld_delim + 1, v_len);

        else -- if there is not a smallest then all are equal  because it is the last cell in the file
          v_amount := v_pos_fld_delim - v_pos_start_cell;
          if (v_amount > 0 ) then
            dbms_lob.copy(v_cell_data, p_data, v_amount, 1, v_pos_start_cell);

            if (v_quoted_val) then
              v_amount := dbms_lob.getlength(v_cell_data) - 2 * v_len_qoute_str;
              dbms_lob.copy(v_cell_data2, v_cell_data, v_amount, 1, v_len_qoute_str + 1);
              v_cell_data := v_cell_data2;
            else
              v_pos_line_delim := dbms_lob.instr(v_cell_data, p_line_delim);
              if (v_pos_line_delim <> 0) then
                dbms_lob.copy(v_cell_data2, v_cell_data, 1, 1, v_pos_line_delim);
                v_cell_data := v_cell_data2;
              end if;
            end if;

            set_cell(p_rid, v_row_num, v_col_num, v_cell_data, p_quote);
          end if;
          v_exit := true;
        end if;
        dbms_lob.trim(v_cell_data, 0);
        exit when v_exit;
      end loop;

      return v_row_num;
    end csv_to_cells;

  procedure set_grid_count(p_rid in imp_run.imp_run_id%type) as
  pragma autonomous_transaction;
    begin
      update imp_run set grid_count = (
        select count(*) from imp_run_grid where imp_run_id = p_rid)
      where imp_run_id = p_rid;
      commit;
    end;

  procedure set_fill_grid_finish_ts(p_rid imp_run.imp_run_id%type) as
  pragma autonomous_transaction;
    begin
      update imp_run set csv_parse_finished_ts = current_date
      where imp_run_id = p_rid;

      commit;
    end set_fill_grid_finish_ts;

  function fill_grid(
    rid imp_run.imp_run_id%type,
    p_update_imp_status in boolean default true)
    return number as

    v_numrows number;
    begin

      select count(*) into v_numrows
      from imp_run_grid where imp_run_id = rid and col_num = 1;

      if p_update_imp_status then
        set_grid_count(rid);
        set_fill_grid_finish_ts(rid);
      end if;

      return v_numrows;
    end fill_grid;


    function col_num(
        rid in imp_run.imp_run_id%type,
        cid in imp_column.imp_column_id%type) return imp_run_grid.col_num%type
    as
        v_col_num imp_run_grid.col_num%type;
    begin
        select pkg_imp_utils.get_col_num(rid, name)
          into v_col_num
          from imp_column 
         where imp_column_id = cid;

        return v_col_num;
    exception
        when no_data_found then
            return null;
    end col_num;

  function cell_value(
    rid imp_run.imp_run_id%type,
    rn imp_run_grid.row_num%type,
    cn imp_run_grid.col_num%type) return imp_run_grid.data%type as
    val clob;
    begin
      val := null;
      select data into val from imp_run_grid
      where imp_run_id = rid and row_num = rn and col_num = cn;
      return val;
      exception
      when others then
      return null;
    end cell_value;

  procedure cell_value(
    rid imp_run.imp_run_id%type,
    rn  imp_run_grid.row_num%type,
    cn  imp_run_grid.col_num%type,
    val in out nocopy clob) as
  begin
    select nvl(clob_data, data) into val from imp_run_grid
    where imp_run_id = rid and row_num = rn and col_num = cn;
  exception
    when others then
      val := null;
  end cell_value;

  procedure drop_pks(rid number) as
  pragma autonomous_transaction;
    begin
      delete from imp_run_entity_pk where imp_run_id = rid;
      commit;
    end drop_pks;

  procedure add_pk(
    rid number, eid number, rnum number, pkid number, v_inserted number) as
  pragma autonomous_transaction;
    begin
      insert into imp_run_entity_pk (imp_run_id,imp_entity_id,row_num,pk, is_inserted)
      values (rid,eid,rnum,pkid, v_inserted);
      commit;
    end add_pk;


  function run_entity_plsql(p_pksql in out nocopy clob) return list_id as
    v_ids list_id;
    v_err_code number;
    v_err_msg varchar2(2000);
    begin
      execute immediate to_char(p_pksql) using out v_ids;

      if (v_ids is null) then
        v_ids := list_id();
      end if;

      return v_ids;
      exception
      -- workaround for Oracle's inability to propagate user defined exception name from dynamic sql
      -- see http://www.sql.ru/forum/actualthread.aspx?tid=463162
      when others then
      v_err_code := sqlcode;
      v_err_msg := sqlerrm;

      if (v_err_code = 1 and v_err_msg = 'User-Defined Exception') then
        --seems this is e_skip_row exception, skip this row
        raise e_skip_row;
      else
        raise;
      end if;
    end run_entity_plsql;

  function is_select_statement(p_sql in varchar2) return boolean as
    begin
      return lower(ltrim(p_sql)) like 'select%' and regexp_like(p_sql, ';\z');
    end is_select_statement;


    procedure fill_single_entity_pks(
        p_cur_imp_run in cur_imp_run%rowtype,
        p_imp_entity  in cur_imp_entity%rowtype)
    as
        v_sql_text imp_entity.sql_text%type;
        v_rows_fetched number;
        cur_pks sys_refcursor;
        v_value number;
        v_is_plsql boolean;
        v_ids list_id;
        v_i number;
        v_error_message imp_run_error.error_msg%type;

    begin
        for rec in (select distinct row_num
                      from imp_run_grid
                     where imp_run_id = p_cur_imp_run.imp_run_id
                       and row_num > p_cur_imp_run.start_row
                     order by row_num)
        loop 
            v_sql_text := p_imp_entity.sql_text;
            buld_update_ent_sql(v_sql_text, p_imp_entity.imp_entity_id, rec.row_num, p_cur_imp_run);

            begin
                v_rows_fetched := 0;

                if is_select_statement(to_char(v_sql_text)) then
                    open cur_pks for to_char(regexp_replace(v_sql_text, ';\z'));
                    loop
                        fetch cur_pks into v_value;
                        exit when cur_pks%notfound;

                        v_rows_fetched := v_rows_fetched + 1;
                        exit when p_cur_imp_run.imp_action_id = c_imp_action_insert;

                        add_pk(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, rec.row_num, v_value, 0);
                    end loop;
                    close cur_pks;

                    v_is_plsql := false;
                    v_ids := list_id();
                else
                    v_ids := run_entity_plsql(v_sql_text);

                    if p_cur_imp_run.imp_action_id <> c_imp_action_insert then
                        v_i := v_ids.first;

                        while v_i is not null loop
                            add_pk(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, rec.row_num, v_ids(v_i), 0);
                            v_i := v_ids.next(v_i);
                        end loop;
                    end if;

                    v_is_plsql := true;
                end if;

                if (p_cur_imp_run.imp_action_id = c_imp_action_update) and ((not(v_is_plsql) and v_rows_fetched = 0) or (v_is_plsql and v_ids.count = 0)) then
                    -- update only and xitor to update not found
                    write_error(p_cur_imp_run.imp_run_id, 'No entity to update with "Update" Import action, Entity Name: ' || p_imp_entity.entity_name,
                                c_et_pk, rec.row_num, v_sql_text);

                elsif (p_cur_imp_run.imp_action_id = c_imp_action_insert) and ((not(v_is_plsql) and v_rows_fetched > 0) or (v_is_plsql and v_ids.count > 0)) then
                    -- insert_only and xitor already exists
                    write_error(p_cur_imp_run.imp_run_id, '', c_et_xitor_exists, rec.row_num, v_sql_text);

                elsif (p_cur_imp_run.imp_action_id in (c_imp_action_insert_update, c_imp_action_insert))
                    and ((not(v_is_plsql) and v_rows_fetched = 0) or (v_is_plsql and v_ids.count = 0)) then
                    -- insert or insert/update and xitor not found
                    v_sql_text := build_insert_ent_sql(p_imp_entity.xitor_type_id, p_imp_entity.imp_entity_id, rec.row_num, p_cur_imp_run);
                    v_sql_text:= replace(v_sql_text, chr(13), '');
                    v_sql_text:= replace(v_sql_text, chr(10), ' ');
                    v_value := run_sql_ret(p_cur_imp_run.imp_run_id, v_sql_text, c_et_new_xitor, rec.row_num);

                    if v_value is not null then
                        add_pk(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, rec.row_num, v_value, 1);
                    end if;
                end if;

            exception
                when e_skip_row then
                    null;

                when others then
                    if cur_pks%isopen then
                        close cur_pks;
                    end if;

                    v_error_message := dbms_utility.format_error_stack;

                    if (instr(v_error_message, 'ORA-06539') > 0) then
                        v_error_message := substr(v_error_message, 1, instr(v_error_message, 'ORA-06539') - 1);
                    end if;

                    write_error(p_cur_imp_run.imp_run_id, v_error_message, c_et_pk, rec.row_num, v_sql_text);
            end;
        end loop;
    end fill_single_entity_pks;


    procedure fill_entity_pks(
        p_process_id in process.process_id%type,
        p_imp_run_id in imp_run.imp_run_id%type)
    as
        v_imp_run cur_imp_run%rowtype;
        v_is_select_statement boolean;
    begin
        set_status(p_process_id, p_imp_run_id, c_s_pk);
        drop_pks(p_imp_run_id);

        open cur_imp_run(p_imp_run_id);
        fetch cur_imp_run into v_imp_run;
        close cur_imp_run;

        for rec_imp_entity in cur_imp_entity(v_imp_run.imp_spec_id) loop
            v_is_select_statement := is_select_statement(to_char(rec_imp_entity.sql_text));

            if v_imp_run.use_fimport = 1 and v_is_select_statement then
                pkg_fimp_run.fill_single_entity_pks(v_imp_run, rec_imp_entity, false);
            else
                if pkg_fimp_run.is_compare and v_imp_run.use_fimport = 0 and v_is_select_statement then 
                    pkg_fimp_run.fill_single_entity_pks(v_imp_run, rec_imp_entity, true);
                end if;

                fill_single_entity_pks(v_imp_run, rec_imp_entity);

                if pkg_fimp_run.is_compare and v_is_select_statement then
                    pkg_fimp_run.save_diff_data(p_imp_run_id, rec_imp_entity.imp_entity_id);
                end if;
            end if;
        end loop;

        pkg_imp_utils.set_entity_pk_count(p_imp_run_id);
        pkg_imp_utils.set_fill_entity_pks_finish_ts(p_imp_run_id);
    end fill_entity_pks;

  procedure update_autogenerated_xitor_key(
    p_rid imp_run.imp_run_id%type,
    p_xid in xitor.xitor_id%type,
    p_row_num in number) is
    --      pragma autonomous_transaction;

    v_err_msg clob;
    v_new_key xitor.xitor_key%type;
    begin
      v_new_key := pkg_xitor.generate_xitor_key(p_xid);
      --update ancestor set p_xitor_key = v_new_key where parent_id = p_xid;
      --update ancestor set c_xitor_key = v_new_key where child_id = p_xid;
      --      commit;
      exception
      when others then
      v_err_msg := 'Error when generating xitor key for xitor_id = "';
      v_err_msg := v_err_msg || p_xid || '" ' || chr(10) || dbms_utility.format_error_stack;
      write_error(p_rid => p_rid,
                  p_msg => v_err_msg,
                  p_err_type_id => c_et_autokey,
                  p_row_num => p_row_num,
                  p_entity_id => p_xid);
    end update_autogenerated_xitor_key;

  procedure generate_xitor_keys(p_rid imp_run.imp_run_id%type) as
    cursor cur_pks(p_rid imp_run.imp_run_id%type) is
      select epk.pk, epk.row_num
        from imp_entity e, xitor_type xt, imp_run_entity_pk epk, imp_run irun 
       where e.imp_spec_id = irun.imp_spec_id
         and xt.xitor_type_id = e.xitor_type_id
         and xt.is_autokey = 1 and epk.imp_run_id = p_rid
         and epk.imp_entity_id = e.imp_entity_id 
         and epk.row_num > irun.start_row
         and irun.imp_run_id = p_rid and epk.pk is not null
         and epk.is_inserted = 1 
         and e.imp_entity_id in (select ierf.imp_entity_id
                                   from imp_entity_req_field ierf, xitor_req_field xrf
                                  where xrf.xitor_req_field_id = ierf.xitor_req_field_id
                                    and xrf.field_name = c_trackor_key
                                    and imp_column_id is null and value = c_auto_generation)--used auto_generation 
       order by e.order_number, epk.row_num;
    begin
      for rec_pk in cur_pks(p_rid) loop
        update_autogenerated_xitor_key(p_rid, rec_pk.pk, rec_pk.row_num);
      end loop;
      -- since key update_autogenerated_xitor_key is no longer autonomous transaction (0431_pkg_imp_run.sql)
      -- I need to commit here
      commit;
    end generate_xitor_keys;

  procedure rollback_xitor_creation(p_xid in xitor.xitor_id%type) is
  pragma autonomous_transaction;

    begin
      pkg_xitor.drop_xitor(p_xid);

      -- clear audit log
      delete from audit_log
      where pk = p_xid and (table_name = 'XITOR' or
                            (table_name = 'CONFIG_VALUE' and (
                                                               /* ensure we are not deleting config values of static xitor types (xitor_keys may intersect) */
                                                               select xt.is_static_definition from xitor_type xt, config_field cf
                                                               where xt.xitor_type_id = cf.xitor_type_id
                                                                     and cf.config_field_id = column_name) = 0));

      commit;
    end rollback_xitor_creation;


    procedure exec_trigger(
        p_rtid in rule.rule_type_id%type,
        p_xtid in xitor_type.xitor_type_id%type,
        p_pk in xitor.xitor_id%type,
        p_rid in imp_run.imp_run_id%type,
        p_row_num in number) is
    pragma autonomous_transaction;

        v_retval varchar2(4000);
        v_err_msg clob;
        v_err_type number(2);
        v_rt varchar2(20);
        v_pk_filed varchar2(20);
        v_msg_spos integer;
    begin
        v_retval := pkg_ruleator.execute_trigger(p_rtid, p_xtid, p_pk);

        if v_retval is not null then
            raise_application_error(-20000, v_retval);
        end if;

    commit;

    exception
        when others then
            rollback;

        if p_rtid = pkg_ruleator.c_type_trackor_created then
            rollback_xitor_creation(p_pk);
            v_rt := 'New Xitor';
            v_pk_filed := 'XITOR_ID';
            v_err_type := c_et_new_xitor_rule;

        elsif p_rtid = pkg_ruleator.c_type_trackor_updated then
            v_rt := 'Update Xitor';
            v_pk_filed := 'XITOR_ID';
            v_err_type := c_et_update_xitor_rule;

        elsif p_rtid = pkg_ruleator.c_type_trackor_created_updated then
            v_rt := 'After Trackor Create/Update';
            v_pk_filed := 'XITOR_ID';
            v_err_type := c_et_create_update_xitor_rule;

        elsif p_rtid = pkg_ruleator.c_type_wp_updated then
            v_rt := 'WP Update';
            v_pk_filed := 'WP_WORKPLAN_ID';
            v_err_type := c_et_wp_rule;
        end if;

        v_msg_spos := instr(lower(sqlerrm), '<errormsg>');

        if v_msg_spos > 0 then
            v_msg_spos := v_msg_spos + 10;
            v_err_msg := substr(sqlerrm, v_msg_spos, instr(lower(sqlerrm),'</errormsg>') - v_msg_spos);
        else
            v_err_msg := 'Error when executing "'|| v_rt || '" rule. ';
            v_err_msg := v_err_msg || chr(10) || v_retval || chr(10) || sqlerrm;
        end if;

        v_err_msg := v_err_msg || chr(10) || v_pk_filed || ' = ' || p_pk || chr(10)
                     || dbms_utility.format_error_stack || ' ' || dbms_utility.format_error_backtrace;

        write_error(p_rid => p_rid,
                    p_msg => v_err_msg,
                    p_err_type_id => v_err_type,
                    p_row_num => p_row_num,
                    p_entity_id => p_pk);
    end exec_trigger;

  procedure exec_wp_triggers(p_rid imp_run.imp_run_id%type) as
    begin
      for rec in (
      select epk.pk, epk.row_num, epk.is_inserted, wp.template_workplan_id
      from imp_entity e, imp_run_entity_pk epk, imp_run irun, wp_workplan wp
      where e.imp_spec_id = irun.imp_spec_id and e.xitor_type_id = 99
            and epk.imp_run_id = p_rid and wp.wp_workplan_id = epk.pk
            and epk.imp_entity_id = e.imp_entity_id and epk.row_num > irun.start_row
            and irun.imp_run_id = p_rid and epk.pk is not null
            and is_inserted = 0 order by epk.row_num) loop

        exec_trigger(31, rec.template_workplan_id, rec.pk, p_rid, rec.row_num);
      end loop;
    end exec_wp_triggers;

    procedure exec_xitor_triggers(p_rid imp_run.imp_run_id%type) as
    begin
        for rec in (
            select epk.pk, epk.row_num, epk.is_inserted, xt.xitor_type_id
              from imp_entity e, xitor_type xt, imp_run_entity_pk epk, imp_run irun
             where e.imp_spec_id = irun.imp_spec_id
               and xt.xitor_type_id = e.xitor_type_id
               and xt.is_static_definition = 0
               and epk.imp_run_id = p_rid
               and epk.imp_entity_id = e.imp_entity_id
               and epk.row_num > irun.start_row
               and irun.imp_run_id = p_rid
               and epk.pk is not null
             order by epk.row_num) loop

            if (rec.is_inserted = 1) then
                exec_trigger(18, rec.xitor_type_id, rec.pk, p_rid, rec.row_num);
            else
                exec_trigger(19, rec.xitor_type_id, rec.pk, p_rid, rec.row_num);
            end if;

            exec_trigger(80, rec.xitor_type_id, rec.pk, p_rid, rec.row_num);
        end loop;
    end exec_xitor_triggers;

  procedure drop_dms(rid number) as
  pragma autonomous_transaction;
    begin
      delete from imp_run_data_map_sql where imp_run_id = rid;
      commit;
    end drop_dms;

  procedure add_dm(rid number, dmid number, vsql clob, dtsql clob) as
  pragma autonomous_transaction;
    begin
      insert into imp_run_data_map_sql (imp_run_id,imp_data_map_id,data_sql)
      values (rid,dmid,dtsql);
      commit;
    end add_dm;

  procedure fill_datamap_sql(pr_spec in cur_imp_spec%rowtype) as
    v_varval varchar2(4000);
    v_dtsql clob;
    begin
      set_status(pr_spec.process_id, pr_spec.imp_run_id, c_s_gen_sql);
      drop_dms(pr_spec.imp_run_id);

      for dm in (
      select m.imp_data_map_id, m.imp_data_type_id, t.name, col.name col_name
      from imp_data_map m join imp_data_type t
          on (t.imp_data_type_id = m.imp_data_type_id)
        left outer join imp_column col on (col.imp_column_id = m.imp_column_id)
      where m.imp_spec_id = pr_spec.imp_spec_id
            and t.name not in ('Relation', 'Relation by xitor_id', 'Configurable Field')) loop

        select sql_text into v_dtsql from imp_data_type
        where imp_data_type_id = dm.imp_data_type_id;

        for dtparam in (
        select dmv.*,dtp.sql_parameter
        from imp_data_type_param dtp, imp_data_type_param_value dmv
        where dtp.param_type <> 11 and dtp.imp_data_type_param_id = dmv.imp_data_type_param_id
              and dmv.imp_data_map_id = dm.imp_data_map_id) loop
          if dtparam.value = 'USER_ID' then
            v_varval := pr_spec.user_id;
          elsif dtparam.value = 'PROGRAM_ID' then
            v_varval := pr_spec.program_id;
          elsif dtparam.value = 'DATE_FORMAT' then
            v_varval := pr_spec.date_format;
          else
            v_varval := dtparam.value;
          end if;
          v_dtsql := ireplace(v_dtsql, ':'||dtparam.sql_parameter, v_varval);
        end loop;

        v_dtsql := ireplace(v_dtsql,'[USER_ID]',to_clob(pr_spec.user_id));
        v_dtsql := ireplace(v_dtsql,'[PROGRAM_ID]',to_clob(pr_spec.program_id));
        v_dtsql := ireplace(v_dtsql,'[DATE_FORMAT]',pr_spec.date_format);
        v_dtsql := ireplace(v_dtsql,'[TIME_FORMAT]',pr_spec.time_format);
        v_dtsql := ireplace(v_dtsql,'[COLUMN_NAME]',dm.col_name);

        add_dm(pr_spec.imp_run_id, dm.imp_data_map_id, null, v_dtsql);
      end loop;

    end fill_datamap_sql;

    procedure import_data(pr_spec in cur_imp_spec%rowtype) as
        v_replace_value clob;
        v_sql clob;
        v_value clob;
        v_cell_value clob;
        v_rowcount number := 0;
        v_dt_param_val_num number;
        v_dt_param_val_num2 number;
        v_dt_param_val_num3 number;
        v_dt_param_val_char varchar2(500);
        v_xkey xitor.xitor_key%type;
        v_xid xitor.xitor_id%type;

        cursor cur_data(
            p_run_id in imp_run.imp_run_id%type,
            p_start_row in imp_run.start_row%type) is
            select pk.row_num, pk.pk,
                   pk.imp_entity_id,
                   m.imp_data_map_id,
                   m.imp_data_type_id,
                   m.imp_column_id,
                   m.sql_text,
                   t.name,
                   coalesce(to_clob(gc.data), gc.clob_data) as value
              from imp_run_entity_pk pk
              join imp_entity ent on ent.imp_entity_id = pk.imp_entity_id
              join imp_data_map m on m.imp_entity_id = pk.imp_entity_id
              join imp_data_type t on t.imp_data_type_id = m.imp_data_type_id
              left join imp_column ic on m.imp_column_id = ic.imp_column_id
              left join imp_run_grid g on g.data = ic.name
                                      and g.row_num = p_start_row
                                      and g.imp_run_id = pk.imp_run_id
              left join imp_run_grid gc on gc.imp_run_id = pk.imp_run_id
                                       and g.col_num = gc.col_num
                                       and pk.row_num = gc.row_num
             where pk.imp_run_id = p_run_id
               and pk.row_num > p_start_row
               and pk.pk is not null
             order by ent.order_number, pk.row_num, m.order_number;

        cursor cur_cols(p_dm_id in imp_data_map.imp_data_map_id%type) is
            select dmv.value, dtp.sql_parameter
              from imp_data_type_param dtp, imp_data_type_param_value dmv
             where dtp.param_type = 11 
               and dmv.imp_data_map_id = p_dm_id
               and dtp.imp_data_type_param_id = dmv.imp_data_type_param_id;
    begin
        set_status(pr_spec.process_id, pr_spec.imp_run_id, c_s_imp_data);

        for rec in cur_data(pr_spec.imp_run_id, pr_spec.start_row) loop
            if rec.imp_column_id is not null then
                v_value := rec.value;
            else
                v_sql := rec.sql_text;

                for param in (select imp_column_id, parameter_value, sql_parameter 
                                from imp_data_map_param
                               where imp_data_map_id = rec.imp_data_map_id)
                loop
                    if param.imp_column_id is not null then
                        cell_value(pr_spec.imp_run_id, rec.row_num, col_num(pr_spec.imp_run_id, param.imp_column_id), v_replace_value);
                    elsif param.parameter_value = 'USER_ID' then
                        v_replace_value := to_clob(pr_spec.user_id);
                    elsif param.parameter_value = 'PROGRAM_ID' then
                        v_replace_value := to_clob(pr_spec.program_id);
                    elsif param.parameter_value = 'DATE_FORMAT' then
                        v_replace_value := to_clob(pr_spec.date_format);
                    elsif param.parameter_value = 'IMP_RUN_ID' then
                        v_replace_value := to_clob(pr_spec.imp_run_id);
                    elsif param.parameter_value = 'ROW_NUM' then
                        v_replace_value := to_clob(rec.row_num);
                    elsif param.parameter_value = 'ENTITY_PK' then
                        v_replace_value := to_clob(rec.pk);
                    else
                        v_replace_value := param.parameter_value;
                    end if;

                    v_sql := ireplace(v_sql, ':'||param.sql_parameter, v_replace_value);
                end loop;

                v_sql := 'declare VALUE varchar2(4000); begin :VALUE := null; '||v_sql||' end;';

                begin
                    v_value := run_sql_ret_char(pr_spec.imp_run_id, v_sql, c_et_val, rec.row_num, rec.imp_data_map_id);
                exception
                    when e_skip_row then
                        continue;
                end;
            end if;

            -- import static imp data types
            if rec.name = 'Configurable Field' then
                begin
                    select to_number(v.value)
                      into v_dt_param_val_num
                      from imp_data_type_param_value v 
                      join imp_data_type_param p on (p.imp_data_type_param_id = v.imp_data_type_param_id)
                     where v.imp_data_map_id = rec.imp_data_map_id
                       and p.sql_parameter = 'ConfigurableFieldID';
                exception
                    when no_data_found then
                        write_error(p_rid => pr_spec.imp_run_id,
                                    p_msg => pkg_label.get_label_system(18072),
                                    p_err_type_id => c_et_imp_conf,
                                    p_row_num => rec.row_num,
                                    p_entity_id => rec.imp_data_map_id);
                end;

                begin
                    select to_number(v.value)
                      into v_dt_param_val_num2
                      from imp_data_type_param_value v
                      join imp_data_type_param p on (p.imp_data_type_param_id = v.imp_data_type_param_id)
                     where v.imp_data_map_id = rec.imp_data_map_id
                       and p.sql_parameter = 'AddVTableValueIfNotExists';
                exception
                    when no_data_found then
                        v_dt_param_val_num2 := 0;
                end;

                begin
                    select to_number(v.value)
                      into v_dt_param_val_num3
                      from imp_data_type_param_value v 
                      join imp_data_type_param p on (p.imp_data_type_param_id = v.imp_data_type_param_id)
                     where v.imp_data_map_id = rec.imp_data_map_id
                      and p.sql_parameter = 'ForceUpdate';
                exception
                    when no_data_found then
                        v_dt_param_val_num3 := 0;
                end;

                imp_cf(v_dt_param_val_num, v_dt_param_val_num2, v_dt_param_val_num3, rec.pk, v_value,
                       pr_spec.date_format, pr_spec.time_format, pr_spec.program_id,
                       pr_spec.imp_run_id, rec.row_num, rec.imp_data_map_id);

            elsif rec.name in ('Relation', 'Relation by xitor_id') then
                begin
                    select to_number(v.value) 
                      into v_dt_param_val_num
                      from imp_data_type_param_value v
                      join imp_data_type_param p on p.imp_data_type_param_id = v.imp_data_type_param_id
                     where v.imp_data_map_id = rec.imp_data_map_id
                       and p.param_name = 'Parent xitor type';

                    select v.value 
                      into v_dt_param_val_char
                      from imp_data_type_param_value v 
                      join imp_data_type_param p on p.imp_data_type_param_id = v.imp_data_type_param_id
                     where v.imp_data_map_id = rec.imp_data_map_id
                       and p.param_name = 'Delete existing relations';
                exception
                    when no_data_found then
                        write_error(p_rid => pr_spec.imp_run_id,
                                    p_msg => pkg_label.get_label_system(18072),
                                    p_err_type_id => c_et_imp_conf,
                                    p_row_num => rec.row_num,
                                    p_entity_id => rec.imp_data_map_id);
                end;

                begin
                    if rec.name = 'Relation' then
                        v_xkey := v_value;
                        v_xid := null;
                    else
                        v_xkey := null;
                        v_xid := to_number(v_value);
                    end if;

                    create_rel(v_dt_param_val_num, v_dt_param_val_char, rec.pk,
                               v_xkey, pr_spec.program_id, pr_spec.imp_run_id,
                               rec.row_num, rec.imp_data_map_id, v_xid);
                exception
                    when value_error then
                        write_error(p_rid => pr_spec.imp_run_id,
                                    p_msg => pkg_label.format_wrapped(18073, pkg_label.list_label_params('parent_trackor_id' => v_value,
                                                                                                         'child_trackor_id' => rec.pk)),
                                    p_err_type_id => c_et_data,
                                    p_row_num => rec.row_num,
                                    p_dmid => rec.imp_data_map_id,
                                    p_entity_id => rec.pk);
                end;
            elsif rec.name = 'EFile' then
                begin
                    select to_number(value)
                      into v_dt_param_val_num
                      from imp_data_type_param_value
                     where imp_data_map_id = rec.imp_data_map_id;
                exception
                    when no_data_found then
                        write_error(p_rid => pr_spec.imp_run_id,
                                    p_msg => pkg_label.get_label_system(18072),
                                    p_err_type_id => c_et_imp_conf,
                                    p_row_num => rec.row_num,
                                    p_dmid => rec.imp_data_map_id,
                                    p_entity_id => rec.pk);
                end;

                imp_efile(v_dt_param_val_num, rec.pk, pr_spec.imp_run_id, rec.row_num,
                          col_num(pr_spec.imp_run_id, rec.imp_column_id), rec.imp_data_map_id, pr_spec.start_row);
            else
                -- import dynamic imp data types
                select data_sql 
                  into v_sql 
                  from imp_run_data_map_sql
                 where imp_run_id = pr_spec.imp_run_id
                   and imp_data_map_id = rec.imp_data_map_id;

                v_sql := ireplace(v_sql, ':ENTITY_PK', to_clob(rec.pk));
                v_sql := ireplace(v_sql, ':VALUE', v_value);
                v_sql := 'begin ' || v_sql || ' end;';

                for cols in cur_cols(rec.imp_data_map_id) loop
                    cell_value(pr_spec.imp_run_id, rec.row_num,
                               col_num(pr_spec.imp_run_id, to_number(cols.value)), v_cell_value);
                    v_sql := ireplace(v_sql, ':' || cols.sql_parameter, v_cell_value);
                end loop;

                run_sql(pr_spec.imp_run_id, v_sql, c_et_data, rec.row_num, rec.imp_data_map_id);
            end if;

            if v_rowcount <> rec.row_num - pr_spec.start_row then
                v_rowcount := rec.row_num - pr_spec.start_row;
                set_rows(pr_spec.imp_run_id, v_rowcount);
            end if;
        end loop;
    end import_data;

  procedure process_start(p_proc_id process.process_id%type) as
  pragma autonomous_transaction;
    begin
      update process
      set actual_start_date = current_date,
        scheduler_start = current_date
      where process_id = p_proc_id;
      commit;
    end process_start;

  procedure process_finish(p_proc_id process.process_id%type) as
  pragma autonomous_transaction;
    begin
      update process
      set scheduler_end = current_date,
        runtime = round((current_date - scheduler_start) * 86400)
      where process_id = p_proc_id;

      delete from process_run where process_id = p_proc_id;

      commit;
    end process_finish;

  procedure incremental_import(
    p_proc_id process.process_id%type,
    p_is_incremental imp_run.is_incremental%type)
  as
    v_imp_run_id number;
    extproc varchar2(2000);
    v_errmsg clob;
    r_spec cur_imp_spec%rowtype;
    v_rule_retval varchar2(4000);
    v_dgid dropgrid.dropgrid_id%type;
    begin
      pkg_code_coverage.start_coverage_data_collection;
      
      select imp_run_id into v_imp_run_id
      from imp_run
      where process_id = p_proc_id;

      process_start(p_proc_id);
      set_delimiters(v_imp_run_id);
      prepare_import(v_imp_run_id);

      open cur_imp_spec(v_imp_run_id);
      fetch cur_imp_spec into r_spec;
      close cur_imp_spec;

      pkg_sec.set_cu(r_spec.user_id);
      pkg_audit.call_stack_add_routine(2, v_imp_run_id, r_spec.imp_spec_id);

      begin
        set_status(p_proc_id, v_imp_run_id, c_s_rules_imp_start);
        v_rule_retval := pkg_ruleator.execute_trigger(38, r_spec.imp_spec_id, v_imp_run_id);

        if (v_rule_retval is not null) then
          write_error(v_imp_run_id, v_rule_retval, pkg_imp_run.c_et_imp_start_rule, 0);
        end if;

      exception
        when others then
          v_errmsg := dbms_utility.format_error_stack;
          write_error(v_imp_run_id, v_errmsg, c_et_imp_start_rule, 0);
          set_status(p_proc_id, v_imp_run_id, c_s_interrupted);
          pkg_audit.call_stack_del_routine(2, v_imp_run_id, r_spec.imp_spec_id);
          return;
      end;

      if (p_is_incremental = 1) then
        pkg_imp_utils.prepare_imp_delta(v_imp_run_id);
        pkg_imp_utils.v_is_incremental := 1;
      end if;

      select external_proc into extproc from imp_spec
      where imp_spec_id = (select imp_spec_id from imp_run where imp_run_id = v_imp_run_id);

      if extproc is not null then -- execute external import and exit
        set_status(p_proc_id, v_imp_run_id, c_s_imp_data);
        begin
          extproc := 'begin '||extproc||' end;';
          execute immediate extproc using in v_imp_run_id;
          exception
          when others then
          v_errmsg := 'Error executing external import: "' || extproc ||
                      '"' || chr(10) || dbms_utility.format_error_stack;

          write_error(v_imp_run_id, v_errmsg, pkg_imp_run.c_extimp, 0);
        end;
      else
        fill_entity_pks(p_proc_id, v_imp_run_id);
        fill_datamap_sql(r_spec);
        import_data(r_spec);
        generate_xitor_keys(v_imp_run_id);
        exec_xitor_triggers(v_imp_run_id);
        exec_wp_triggers(v_imp_run_id);
      end if;

      begin
        set_status(p_proc_id, v_imp_run_id, c_s_rules_imp_finish);
        v_rule_retval := pkg_ruleator.execute_trigger(37, r_spec.imp_spec_id, v_imp_run_id);
        if (v_rule_retval is not null) then
          write_error(v_imp_run_id, v_rule_retval, pkg_imp_run.c_et_imp_complete_rule, 0);
        end if;
      exception
        when others then
          v_errmsg := dbms_utility.format_error_stack;
          write_error(v_imp_run_id, v_errmsg, pkg_imp_run.c_et_imp_complete_rule, 0);
      end;

      delete from imp_run_data_map_sql where imp_run_id = v_imp_run_id;
      set_status(p_proc_id, v_imp_run_id, c_s_end);
      pkg_audit.call_stack_del_routine(2, v_imp_run_id, r_spec.imp_spec_id);

      if (r_spec.notify_on_completion = 1) then
        begin
          notify(v_imp_run_id);
          exception
        when others then
          v_errmsg := 'Can''t notify user on import completion: ' || chr(10) || dbms_utility.format_error_stack;
          write_error(v_imp_run_id, v_errmsg, pkg_imp_run.c_extimp, 0);
        end;
      end if;

      begin
        select dropgrid_id into v_dgid
        from dropgrid_sheet where import_process_id = p_proc_id;
      exception
        when no_data_found then
        v_dgid := null;
      end;

      if v_dgid is not null then
        update xitor_audit set dropgrid_id = v_dgid
        where xitor_id in (select pk from imp_run_entity_pk where imp_run_id = v_imp_run_id and is_inserted = 1);
      end if;

      if (not(nvl(r_spec.days_to_keep_parsed_data, 0) > 0)
          and lower(nvl(extproc, 'null')) not like '%pkg_ext_imp.parse_csv%') then
        delete from imp_run_grid_incr where imp_run_id = v_imp_run_id;
        drop_pks(v_imp_run_id);
      end if;

      process_finish(p_proc_id);

       pkg_code_coverage.stop_coverage_data_collection;

    exception
      when others then
        v_errmsg := dbms_utility.format_error_stack || dbms_utility.format_error_backtrace;
        write_error(v_imp_run_id, v_errmsg, c_et_unknown, 0);
        set_status(p_proc_id, v_imp_run_id, c_s_end);
        pkg_audit.call_stack_del_routine(2, v_imp_run_id, r_spec.imp_spec_id);
    end incremental_import;


  procedure import(p_proc_id process.process_id%type)
  is
    begin
      incremental_import(p_proc_id, 0);
    end import;


    procedure buld_update_ent_sql(
        pksql in out nocopy clob,
        v_entid in imp_entity.imp_entity_id%type,
        v_row_num in number,
        v_imp_run in cur_imp_run%rowtype)
    is
        v_value varchar2(4000);
        v_col_num number;
    begin
        for param in (select imp_column_id, parameter_value, sql_parameter
                        from imp_entity_param 
                       where imp_entity_id = v_entid) loop
            if param.imp_column_id is not null then
                v_col_num := col_num(v_imp_run.imp_run_id, param.imp_column_id);
                cell_value(v_imp_run.imp_run_id, v_row_num, v_col_num, v_value);
            else
                if param.parameter_value = 'USER_ID' then
                    v_value := v_imp_run.user_id;
                elsif param.parameter_value = 'PROGRAM_ID' then
                    v_value := v_imp_run.program_id;
                elsif param.parameter_value = 'DATE_FORMAT' then
                    v_value := v_imp_run.date_format;
                elsif param.parameter_value = 'IMP_RUN_ID' then
                    v_value := v_imp_run.imp_run_id;
                elsif param.parameter_value = 'ROW_NUM' then
                    v_value := v_row_num;
                else
                    v_value := param.parameter_value;
                end if;
            end if;

            pksql := ireplace(pksql, ':' || param.sql_parameter, v_value);
        end loop;
    end buld_update_ent_sql;

    function gen_sync_trackor_key_seq_sql(p_xitor_type_id xitor_type.xitor_type_id%type, p_csv_seq_val number) return varchar2 
    as
        c_seq_autokey varchar2(100) := 'seq_autokey_';
    begin
         return 'declare
                     v_cur_seq_val number;
                 begin
                     execute immediate ''select seq_autokey_' || p_xitor_type_id || '.nextval from dual'' into v_cur_seq_val;

                     if ' || p_csv_seq_val || ' > v_cur_seq_val then
                         execute immediate ''alter sequence ' || c_seq_autokey || p_xitor_type_id || ' increment by ''|| (' || p_csv_seq_val || ' - v_cur_seq_val);
                         execute immediate ''select ' || c_seq_autokey || p_xitor_type_id || '.nextval from dual'' into v_cur_seq_val;
                         execute immediate ''alter sequence ' || c_seq_autokey || p_xitor_type_id || ' increment by 1'';
                     end if;
                 end;';
    end;

  function build_insert_ent_sql(
      v_xtid in xitor_type.xitor_type_id%type,
      v_entid in imp_entity.imp_entity_id%type,
      v_row_num in number,
      v_imp_run in cur_imp_run%rowtype) return varchar2
  is
      pksql    clob;
      v_sync_seq_sql varchar2(4000);
      fld  varchar2(4000);
      vlst varchar2(4000);
      val  varchar2(4000);
      tbl  varchar2(255);
      pkf  varchar2(255);
      varval    clob;
      v_col_num number;
      v_xitor   boolean := false;
      v_autokey number;
      v_xitor_key varchar2(4000);
      v_zone_id   varchar2(50);
      v_prog_id   varchar2(50);
      v_xclass_id varchar2(50);
      v_wp_template_id wp_workplan.template_workplan_id%type;
      v_wp_xitor_id    xitor.xitor_id%type;
      v_wp_active      wp_workplan.active%type;
      v_wp_name        wp_workplan.name%type;
      v_wp_start       wp_workplan.workplan_start%type;
      v_wp_finish      wp_workplan.workplan_finish%type;
  begin
      select table_name, key_field into tbl, pkf from xitor_type
       where xitor_type_id = v_xtid;

      if (upper(tbl) = 'XITOR') then
          v_xitor := true;

          select is_autokey into v_autokey from xitor_type
           where xitor_type_id = v_xtid;

          v_xitor_key := 'null';
          v_zone_id := 'null';
          v_prog_id := 'null';
          v_xclass_id := 'null';
      end if;

      for reqfld in (select e.field_name, i.imp_column_id, i.sql_text, i.value, i.imp_entity_req_field_id
                       from imp_entity_req_field i, xitor_req_field e
                      where i.xitor_req_field_id = e.xitor_req_field_id
                        and i.imp_entity_id = v_entid
                        and e.program_id = v_imp_run.program_id) loop

          if reqfld.imp_column_id is not null then
              v_col_num := col_num(v_imp_run.imp_run_id, reqfld.imp_column_id);
              val := cell_value(v_imp_run.imp_run_id, v_row_num, v_col_num);

              if reqfld.field_name = c_trackor_key and v_autokey = 1 and val is not null then
                  v_sync_seq_sql := gen_sync_trackor_key_seq_sql(v_xtid, regexp_substr(val, '\d+$'));--take only the numbers on the right
              end if;

          elsif reqfld.value is not null then
              if reqfld.value = 'USER_ID' then
                  val := v_imp_run.user_id;
              elsif reqfld.value = 'PROGRAM_ID' then
                  val := v_imp_run.program_id;
              elsif reqfld.value = 'DATE_FORMAT' then
                  val := v_imp_run.date_format;
              elsif reqfld.value = c_auto_generation then
                  val := c_auto_generation;    
              else
                  val := reqfld.value;
              end if;

          else
              pksql := reqfld.sql_text;

              for rfparam in (select imp_column_id, parameter_value, sql_parameter
                                from imp_entity_req_field_param
                               where imp_entity_req_field_id = reqfld.imp_entity_req_field_id) loop

                  if rfparam.imp_column_id is not null then
                      v_col_num := col_num(v_imp_run.imp_run_id, rfparam.imp_column_id);
                      cell_value(v_imp_run.imp_run_id, v_row_num, v_col_num, varval);
                  else
                      if rfparam.parameter_value = 'USER_ID' then
                          varval := to_clob(v_imp_run.user_id);
                      elsif rfparam.parameter_value = 'PROGRAM_ID' then
                          varval := to_clob(v_imp_run.program_id);
                      elsif rfparam.parameter_value = 'DATE_FORMAT' then
                          varval := to_clob(v_imp_run.date_format);
                      elsif rfparam.parameter_value = 'ROW_NUM' then
                          varval := to_clob(v_row_num);
                      elsif rfparam.parameter_value = 'IMP_RUN_ID' then
                          varval := to_clob(v_imp_run.imp_run_id);
                      else
                          varval := to_clob(rfparam.parameter_value);
                      end if;
                  end if;

                  pksql := ireplace(pksql, ':' || rfparam.sql_parameter, varval);
              end loop;

              pksql := regexp_replace(pksql, '([^[:graph:]|^[:blank:]])', ' ');
              pksql := regexp_replace(pksql, '--.*[' || chr(13) || ']', '');
              pksql := trim(pksql);

              if substr(pksql, length(pksql), 1) <> ';' then
                  pksql := pksql || ';';
              end if;

              if (reqfld.field_name = c_trackor_key) or (v_xtid = 99 and reqfld.field_name = 'NAME') then
                  pksql := 'declare VALUE varchar2(255); begin ' || pksql || ' end;';
                  val := run_sql_ret_char(v_imp_run.imp_run_id, pksql, c_et_req_val, v_row_num);
              else
                  pksql := 'declare VALUE number; begin ' || pksql || ' end;';
                  val := run_sql_ret(v_imp_run.imp_run_id, pksql, c_et_req_val, v_row_num);
              end if;
          end if;

          if (not(v_xitor)) then
              fld := fld || reqfld.field_name || ',';
              vlst := vlst || '''' || val || ''',';
          elsif (reqfld.field_name = c_trackor_key) then             
              if val = c_auto_generation then
                  v_xitor_key := 'null';
              elsif val is not null then
                  v_xitor_key := '''' || val || '''';
              else
                  v_xitor_key := null; --it means that in the file column have null value and we have to generate an error by xitor creation
              end if;
          elsif (reqfld.field_name = 'ZONE_ID') then
              v_zone_id := val;
          elsif (reqfld.field_name = 'PROGRAM_ID') then
              v_prog_id := val;
          elsif (reqfld.field_name = 'XITOR_CLASS_ID') then
              v_xclass_id := val;
          end if;

          if v_xtid = 99 then
              case reqfld.field_name
                  when 'TEMPLATE_WORKPLAN_ID' then
                      begin
                          v_wp_template_id := val;
                      exception
                          when value_error then
                              write_error(v_imp_run.imp_run_id,
                                          'Use number for "TEMPLATE_WORKPLAN_ID" Entity Required Field instead of "' || val || '"',
                                          c_et_pk, v_row_num, pksql);
                          return null;
                      end;
                  when 'XITOR_ID' then
                      v_wp_xitor_id := val;
                  when 'ACTIVE' then
                      v_wp_active := val;
                  when 'NAME' then
                      v_wp_name := val;
                  when 'WORKPLAN_START' then
                      begin
                          v_wp_start := to_date(val, v_imp_run.date_format);
                      exception
                          when others then
                              write_error(v_imp_run.imp_run_id,
                                          'Use date for "WORKPLAN_START" Entity Required Field instead of "' || val || '"',
                                          c_et_pk, v_row_num, pksql);
                          return null;
                      end;
                  when 'WORKPLAN_FINISH' then
                      begin
                          v_wp_finish := to_date(val, v_imp_run.date_format);
                      exception
                          when others then
                              write_error(v_imp_run.imp_run_id,
                                          'Use date for "WORKPLAN_FINISH" Entity Required Field instead of "' || val || '"',
                                          c_et_pk, v_row_num, pksql);
                          return null;
                      end;
                  else null;
              end case;
          end if;
      end loop;

      if v_xitor then
          pksql := 'declare VALUE number; pk number; v_new_key varchar2(255); ';
          pksql := pksql || 'begin pk := pkg_xitor.new_xitor(';
          pksql := pksql || v_xtid || ',' || v_xitor_key || ',';
          pksql := pksql || ' null,null,null,';
          pksql := pksql || v_prog_id || ',' || v_xclass_id || '); ';
          pksql := pksql || ':VALUE := pk; ';
          pksql := pksql || v_sync_seq_sql; --plsql block for synchronize the sequence
          pksql := pksql || 'end;';
      elsif v_xtid = 99 then
          pksql := 'declare VALUE number; pk number; v_retval varchar2(4000); v_rule_return_str varchar2(4000);';
          pksql := pksql || 'begin ';
          pksql := pksql || ' pk := pkg_wp.assign_wp( ';
          pksql := pksql || ' p_tmpl_wp_id => ' || v_wp_template_id || ',';
          pksql := pksql || ' p_xid        => ' || v_wp_xitor_id || ',';
          pksql := pksql || ' p_name       => ''' || v_wp_name || ''',';
          pksql := pksql || ' p_start   => ''' || v_wp_start  || ''',';
          pksql := pksql || ' p_finish  => ''' || v_wp_finish || ''',';
          pksql := pksql || ' p_active  => ' || v_wp_active || ',';
          pksql := pksql || ' p_rule_return_str => v_rule_return_str); ';
          pksql := pksql || ':VALUE := pk; ';
          pksql := pksql || 'end;';
      else 
          fld := substr(fld, 1, length(fld)-1);
          vlst := substr(vlst, 1, length(vlst)-1);
          pksql := 'declare VALUE number; pk number; v_retval varchar2(4000); ';
          pksql := pksql || 'begin insert into ' || tbl || ' (' || fld || ') ';
          pksql := pksql || 'values (' || vlst || ') returning ' || pkf || ' into pk; ';
          pksql := pksql || ':VALUE := pk; ';
          pksql := pksql || 'end;';          
      end if;

      return pksql;
  end build_insert_ent_sql;

  procedure search_field_info(p_xtid in xitor_type.xitor_type_id%type,
                              p_name in config_field.config_field_name%type,
                              p_fld_id out config_field.config_field_id%type,
                              p_static out config_field.is_static%type,
                              p_type out config_field.data_type%type) is
  begin
      select config_field_id,
             is_static,
             data_type
        into p_fld_id,
             p_static,
             p_type
        from config_field
       where xitor_type_id = p_xtid
         and config_field_name = p_name
         and (is_static = 0 or config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id));
  exception
      when no_data_found then
      p_fld_id := null;
      p_static := null;
      p_type := null;
      return;
  end search_field_info;

  procedure set_numrows(
    p_rid in imp_run.imp_run_id%type,
    p_num_rows in imp_run.num_rows%type) as
  pragma autonomous_transaction;
    begin
      update imp_run set num_rows = p_num_rows where imp_run_id = p_rid;
      commit;
    end;

  procedure prepare_import(p_rid in imp_run.imp_run_id%type) is
    v_numrows number;
    v_use_sql_loader imp_spec.use_sql_loader%type;
    begin
      select imp_s.use_sql_loader into v_use_sql_loader
      from imp_spec imp_s
        join imp_run imp_r on (imp_r.imp_spec_id = imp_s.imp_spec_id)
      where imp_r.imp_run_id = p_rid;

      if (v_use_sql_loader = 1) then
        --            v_numrows := loader_fill_grid(p_rid) - 1;
        raise_application_error(-20000, pkg_label.get_label_system(17686));
      else
        v_numrows := fill_grid(p_rid) - 1;
      end if;

      set_numrows(p_rid, v_numrows);
      set_rows(p_rid, 0);
    end prepare_import;


  procedure imp_cf(
    p_cfid in config_field.config_field_id%type,
    p_add_vtable_value in number,
    p_force_update in number,
    p_ent_pk imp_run_entity_pk.pk%type,
    p_val in out nocopy clob,
    p_date_format in imp_spec.date_format%type,
    p_time_format in imp_spec.time_format%type,
    p_pid in program.program_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in number,
    p_dmid in imp_data_map.imp_data_map_id%type)
  is
    pragma autonomous_transaction;

    v_dt number;
    v_obj_xtid number;
    v_errmsg clob;
    v_cf_name config_field.config_field_name%type;
    v_vtable_id config_field.attrib_v_table_id%type;
    v_ttid config_field.xitor_type_id%type;
    v_val_num number;
    v_val_date date;
  begin
    if (p_val is null) or (dbms_lob.getlength(p_val) <= 0) then
      return;
    end if;

    select data_type, obj_xitor_type_id, config_field_name, attrib_v_table_id, xitor_type_id
    into v_dt, v_obj_xtid, v_cf_name, v_vtable_id, v_ttid
    from config_field where config_field_id=p_cfid;

    if v_dt in (2, 90, 91) then --Date, Date/Time, Time
      case v_dt
        when 2 then v_val_date := to_date(p_val, p_date_format);
        when 90 then v_val_date := to_date(p_val, p_date_format || ' ' || p_time_format);
        when 91 then v_val_date := to_date('01-01-1970','mm-dd-yyyy') + (to_date(p_val, p_time_format) - trunc(to_date(p_val, p_time_format)));
      end case;

      if (p_force_update <> 1 and v_val_date = pkg_config_field_rpt.getValDateByID(p_ent_pk, p_cfid)) then
        return;
      end if;

      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, v_val_date);


    elsif v_dt = 20 then --Trackor Selector

      if (p_force_update <> 1 
          and p_val = pkg_config_field_rpt.getTrackorSelectorVal(p_ent_pk, p_cfid, v_obj_xtid, null, 1)) then
        return;
      end if;

      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, to_char(p_val));


    elsif v_dt = 3 then --Checkbox
      v_val_num := pkg_imp_utils.convert_boolean(p_val);

      if (p_force_update <> 1 and v_val_num = pkg_config_field_rpt.getValNumNLByID(p_ent_pk, p_cfid)) then
        return;
      end if;

      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, v_val_num);


    elsif v_dt in (4, 10) and p_add_vtable_value = 1 then  -- Drop-Down, Selector      
      if (p_force_update <> 1 and p_val = pkg_config_field_rpt.getValStrByID(p_ent_pk, p_cfid)) then
        return;
      end if;

      v_val_num := pkg_dl_support.get_vtabid(v_vtable_id, p_pid, p_val, true);
      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, v_val_num, null, 1);

    elsif v_dt in (5, 7) then  -- Memo, Wiki
      if p_force_update <> 1 
            and dbms_lob.compare(p_val, pkg_config_field_rpt.getFullValMemoByID(p_ent_pk, p_cfid)) = 0 then
        return;
      end if;

      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, p_val);

    else
      if (p_force_update <> 1 and p_val = pkg_config_field_rpt.getValStrByID(p_ent_pk, p_cfid)) then
        return;
      end if;

      pkg_dl_support.set_cf_data(p_cfid, p_ent_pk, p_val);
    end if;

    commit;

  exception
    when others then
      v_errmsg := 'CONFIG_FIELD_NAME: ' || v_cf_name || chr(10) || dbms_utility.format_error_stack;
      write_error(p_rid => p_rid,
                  p_msg => v_errmsg,
                  p_err_type_id => c_et_data,
                  p_row_num => p_row_num,
                  p_dmid => p_dmid,
                  p_entity_id => p_ent_pk,
                  p_col_name => v_cf_name);
      write_field_comment(p_rid, sqlerrm, p_row_num, p_cfid, p_ent_pk, v_ttid, p_dmid, p_val);
      rollback;
  end imp_cf;

  procedure create_rel(
    p_parent_xtid in xitor_type.xitor_type_id%type,
    p_del_existing_rel in varchar2,
    p_ent_pk imp_run_entity_pk.pk%type,
    p_val in xitor.xitor_key%type,
    p_pid in program.program_id%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in number,
    p_dmid in imp_data_map.imp_data_map_id%type,
    p_parent_id in xitor.xitor_id%type default null)
  is

  pragma autonomous_transaction;

    v_rel_type_id relation_type.relation_type_id%type;
    v_parent_id xitor.xitor_id%type;
    v_old_parent_id xitor.xitor_id%type;
    v_xtid xitor_type.xitor_type_id%type;
    v_rel_exist number;
    v_cardinality_id relation_type.cardinality_id%type;
    v_direct_rel relation_type.relation_type_id%type;
    v_child_xitor_key xitor.xitor_key%type;
    v_errmsg clob;

    cursor cur_rel_id(
      p_xid in xitor.xitor_id%type,
      p_parent_rel_id in relation_type.parent_type_id%type) is
      select r.relation_type_id, x.xitor_type_id, r.cardinality_id, x.xitor_key
      from relation_type r, xitor x
      where r.parent_type_id = p_parent_rel_id
            and r.child_type_id = x.xitor_type_id and x.xitor_id = p_xid;

    /* Return all under relations between parent and child */
    cursor cur_direct_relations(
      p_parent_type_id in relation_type.parent_type_id%type,
      p_child_type_id in relation_type.child_type_id%type,
      p_child_id xitor.xitor_id%type) is
      select a.parent_id, rels.relation_type_id, rels.cardinality_id,
        x.xitor_key, x.xitor_type_id
      from ancestor a join xitor x on (a.parent_id = x.xitor_id)
        join (
               select distinct rt2.parent_type_id, rt.relation_type_id, rt.cardinality_id
               from relation_type rt join (
                                            select parent_type_id, relation_type_id
                                            from relation_type
                                            connect by prior child_type_id = parent_type_id
                                            start with child_type_id = p_parent_type_id) rt2
                   on (rt2.relation_type_id = rt.relation_type_id)
               where rt.child_type_id = p_child_type_id) rels
          on (rels.parent_type_id = x.xitor_type_id)
      where a.child_id = p_child_id
      union
      select a.parent_id, rels.relation_type_id, rels.cardinality_id,
        x.xitor_key, x.xitor_type_id
      from ancestor a join xitor x on (a.parent_id = x.xitor_id)
        join (
               select rt2.parent_type_id, rt.relation_type_id, rt.cardinality_id
               from relation_type rt join (
                                            select parent_type_id
                                            from relation_type
                                            connect by child_type_id = prior parent_type_id
                                            start with child_type_id = p_parent_type_id) rt2
                   on (rt.parent_type_id = rt2.parent_type_id)
               where rt.child_type_id = p_child_type_id) rels
          on (rels.parent_type_id = x.xitor_type_id)
      where a.child_id = p_child_id;

    begin
      if (p_parent_id is null) and ((p_val is null) or (dbms_lob.getlength(p_val) <= 0)) then
        return;
      end if;

      open cur_rel_id(p_ent_pk, p_parent_xtid);
      fetch cur_rel_id into v_rel_type_id, v_xtid, v_cardinality_id, v_child_xitor_key;
      if (cur_rel_id%notfound) then /* There are no direct relation between xitor types */
        close cur_rel_id;
        v_errmsg := 'There are no direct relation between xitor types' || chr(10) ||
                    'Child Trackor ID: ' || p_ent_pk || chr(10) || 'Parent Trackor Type ID: ' || p_parent_xtid;
        write_error(p_rid => p_rid,
                    p_msg => v_errmsg,
                    p_err_type_id => c_et_data,
                    p_row_num => p_row_num,
                    p_dmid => p_dmid,
                    p_entity_id => p_ent_pk);
      end if;
      close cur_rel_id;

      if (p_parent_id is null) then
        select x.xitor_id into v_parent_id from xitor x, xitor_type xt
        where x.xitor_type_id = p_parent_xtid and x.xitor_key = p_val
              and xt.xitor_type_id = x.xitor_type_id
              and x.program_id = p_pid;
      else
        v_parent_id := p_parent_id;
      end if;

      /* Check if relation is exists already */
      select count(1) into v_rel_exist from ancestor
      where child_id = p_ent_pk and parent_id = v_parent_id;

      if (v_rel_exist <> 0) then
        return;
      end if;

      /* Delete exisitg above relation */
      for rec in cur_direct_relations(p_parent_xtid, v_xtid, p_ent_pk) loop
        if (p_del_existing_rel = 'Yes') or (rec.cardinality_id = 2) then
          pkg_relation.del_relation(rec.parent_id, p_ent_pk, rec.relation_type_id);
          if (rec.xitor_type_id = p_parent_xtid) then
            v_old_parent_id := rec.parent_id;
          end if;

          if (rec.cardinality_id = 3) then
            /* manually log multiple relation changes */
            begin
              select rt.relation_type_id into v_direct_rel
              from relation_type rt join xitor x
                  on (x.xitor_type_id = rt.parent_type_id)
              where rt.relation_type_id = rec.relation_type_id
                    and child_type_id = v_xtid and x.xitor_id = rec.parent_id;

              pkg_audit.log_multiple_relation(p_ent_pk, rec.relation_type_id);
              exception
              when no_data_found then
              /* we should log only for direct relations */
              null;
            end;
          end if;
        end if;
      end loop;

      if (v_old_parent_id is null) then
        pkg_relation.new_relation(v_parent_id, p_ent_pk, v_rel_type_id);
      else
        pkg_relation.new_relation_plus(v_old_parent_id, v_parent_id,
                                       p_ent_pk, v_rel_type_id, p_pid);
      end if;

      if (v_cardinality_id = 3) then
        pkg_audit.log_multiple_relation(p_ent_pk, v_rel_type_id);
      end if;

      commit;
      exception
      when no_data_found then
      v_errmsg := 'Parent Trackor not found' || chr(10) ||
                  'Parent XITOR_KEY: ' || p_val || chr(10) ||
                  'Parent XITOR_ID: ' || v_parent_id || chr(10) ||
                  'Parent XITOR_TYPE_ID: ' || p_parent_xtid || chr(10) ||
                  'Child XITOR_ID: ' || p_ent_pk || chr(10) ||
                  'Child XITOR_KEY: ' || v_child_xitor_key;
      write_error(p_rid => p_rid,
                  p_msg => v_errmsg,
                  p_err_type_id => c_et_data,
                  p_row_num => p_row_num,
                  p_dmid => p_dmid,
                  p_entity_id => p_ent_pk);
      rollback;
      when too_many_rows then
      v_errmsg := 'Cant found parent Trackor by xitor_key, ' ||
                  'more than one Trackors exists with same xitor_key ' || chr(10) ||
                  'Parent XITOR_KEY: ' || p_val || chr(10) ||
                  'Parent XITOR_ID: ' || v_parent_id || chr(10) ||
                  'Parent XITOR_TYPE_ID: ' || p_parent_xtid || chr(10) ||
                  'Child XITOR_ID: ' || p_ent_pk || chr(10) ||
                  'Child XITOR_KEY: ' || v_child_xitor_key || chr(10) || dbms_utility.format_error_stack;
      write_error(p_rid => p_rid,
                  p_msg => v_errmsg,
                  p_err_type_id => c_et_data,
                  p_row_num => p_row_num,
                  p_dmid => p_dmid,
                  p_entity_id => p_ent_pk);
      rollback;
      when others then
      v_errmsg := 'Cant create relation' || chr(10) || dbms_utility.format_error_stack;
      write_error(p_rid => p_rid,
                  p_msg => v_errmsg,
                  p_err_type_id => c_et_data,
                  p_row_num => p_row_num,
                  p_dmid => p_dmid,
                  p_entity_id => p_ent_pk);
      rollback;
    end create_rel;


  procedure imp_efile(
    p_cfid in config_field.config_field_id%type,
    p_pk imp_run_entity_pk.pk%type,
    p_rid in imp_run.imp_run_id%type,
    p_row_num in integer,
    p_col_num in integer,
    p_dmid in imp_run_error.imp_data_map_id%type,
    p_start_row in imp_run.start_row%type default 1)
  is
    v_clob clob;
    v_thumb_clob clob;
    v_blob blob;
    v_thumb_blob blob;
    v_blob_id blob_data.blob_data_id%type;
    v_filename blob_data.filename%type;
    v_efile_col_name varchar2(100);
    v_thumb_col_name varchar2(100);
    v_errmsg clob;
    v_uid process.user_id%type;
    v_pid process.program_id%type;
    v_log_blob_changes config_field.log_blob_changes%type;
    v_blob_data_len blob_data.blob_data_len%type;
    v_old_blob_data_id blob_data.blob_data_id%type;
    v_old_filename blob_data.filename%type;
    v_old_blob_data_len blob_data.blob_data_len%type;
    v_log_action audit_log.action%type;

  pragma autonomous_transaction;
    begin
      v_filename := cell_value(p_rid, p_row_num, p_col_num);
      if (v_filename is null) then
        return;
      end if;

      v_efile_col_name := cell_value(p_rid, p_start_row, p_col_num) || '_EFILE_DATA';
      begin
        select clob_data into v_clob from imp_run_grid
        where imp_run_id = p_rid and row_num = p_row_num and col_num = pkg_imp_utils.get_col_num(p_rid, v_efile_col_name);
        exception
        when no_data_found then
        return;
      end;

      if (v_clob is null or dbms_lob.getlength(v_clob) = 0) then
        return;
      end if;

      v_thumb_col_name := cell_value(p_rid, p_start_row, p_col_num) || '_EFILE_THUMB_DATA';
      begin
        select clob_data into v_thumb_clob from imp_run_grid
        where imp_run_id = p_rid and row_num = p_row_num and col_num = pkg_imp_utils.get_col_num(p_rid, v_thumb_col_name);
        exception
        when no_data_found then
        v_thumb_clob := null;
      end;

      select log_blob_changes into v_log_blob_changes
      from config_field where config_field_id = p_cfid;

      if (v_log_blob_changes = 1) then
          update blob_data set key_value = null, config_field_id = null
           where key_value = p_pk and config_field_id = p_cfid
          returning blob_data_id, filename, blob_data_len
          into v_old_blob_data_id, v_old_filename, v_old_blob_data_len;
      else
          delete from blob_data where key_value = p_pk and config_field_id = p_cfid
          returning blob_data_id, filename, blob_data_len
          into v_old_blob_data_id, v_old_filename, v_old_blob_data_len;
      end if;

      if (v_thumb_clob is null) then
        insert into blob_data(filename, blob_data, key_value, config_field_id)
        values (v_filename, empty_blob(), p_pk, p_cfid)
        returning blob_data_id into v_blob_id;
      else
        insert into blob_data(filename, blob_data, thumbnail, key_value, config_field_id)
        values (v_filename, empty_blob(), empty_blob(), p_pk, p_cfid)
        returning blob_data_id into v_blob_id;
      end if;

      select blob_data, thumbnail into v_blob, v_thumb_blob from blob_data
      where blob_data_id = v_blob_id for update;

      pkg_lob_utils.hex_to_blob(v_clob, v_blob);
      if (v_thumb_clob is not null) then
        pkg_lob_utils.hex_to_blob(v_thumb_clob, v_thumb_blob);
      end if;

      v_blob_data_len := dbms_lob.getlength(v_blob);
      update blob_data set blob_data_len = v_blob_data_len
      where blob_data_id = v_blob_id;

      pkg_dl_support.set_cf_data_num(p_cfid, p_pk, v_blob_id);

      select p.user_id, p.program_id into v_uid, v_pid
      from process p, imp_run r where p.process_id=r.process_id and r.imp_run_id = p_rid;

      if (v_old_blob_data_id is null) then
        v_log_action := 'I';
      else
        v_log_action := 'U';
      end if;

      pkg_audit.log_changes(
          tablename => 'BLOB_DATA',
          field => p_cfid,
          pkey => p_pk,
          action => v_log_action,
          user_id => v_uid,
          from_number => v_old_blob_data_len,
          to_number => v_blob_data_len,
          from_char => v_old_filename,
          to_char => v_filename,
          from_date => null,
          to_date => null,
          from_blob_data_id => v_old_blob_data_id,
          to_blob_data_id => v_blob_id,
          linenumber => 1,
          prog_id => v_pid,
          xt_id => null);

      commit;
      exception
      when others then
      v_errmsg := 'CONFIG_FIELD_ID: ' || p_cfid || chr(10) || dbms_utility.format_error_stack;
      write_error(p_rid => p_rid,
                  p_msg => v_errmsg,
                  p_err_type_id => c_et_data,
                  p_row_num => p_row_num,
                  p_dmid => p_dmid,
                  p_entity_id => p_pk);
      rollback;
    end imp_efile;

  procedure notify(p_rid in imp_run.imp_run_id%type) is
    v_comments process.comments%type;
    v_warnings_cnt integer;
    v_notification_msg varchar2(1000);
    v_notification_subj varchar2(100);
    v_email users.email%type;
    v_date_fmt varchar2(50);
    v_name imp_spec.name%type;
    v_end_date process.end_date%type;
    v_start_date process.start_date%type;
    v_sender param_system.value%type;
    v_start_row imp_run.start_row%type;
    v_csv_row clob;
    v_empty_csv_row clob;
    v_csv clob;
    b_csv blob;
    blobId number;
    convert_warn integer;
    dest_offset  integer  := 1;
    src_offset   integer  := 1;
    lang_context integer  := DBMS_LOB.default_lang_ctx;
    csid         integer  := DBMS_LOB.default_csid;
    begin
        select s.name,
               p.start_date,
               p.end_date,
               p.comments,
               u.email,
               r.start_row,
               pkg_user.get_date_format(u.user_id),
               (select count(*) from imp_run_error e where e.imp_run_id = r.imp_run_id)
          into v_name,
               v_start_date,
               v_end_date,
               v_comments,
               v_email,
               v_start_row,
               v_date_fmt,
               v_warnings_cnt
          from process p 
          join imp_run r on (p.process_id = r.process_id)
          join imp_spec s on (s.imp_spec_id = r.imp_spec_id)
          join users u on (u.user_id = p.user_id)
         where r.imp_run_id = p_rid;

      v_notification_subj := 'Import "' || v_name || '" finished';
      if (v_warnings_cnt > 0) then
        v_notification_subj := v_notification_subj || ' with warnings';
      end if;
      v_notification_msg := v_notification_subj || c_lb || 'Start date: '
                            || to_char(v_start_date, v_date_fmt || ' hh12:mi AM') || c_lb
                            || 'End date: ' || to_char(v_end_date, v_date_fmt || ' hh12:mi AM') || c_lb
                            || 'Comments: ' || v_comments;

      select value into v_sender from param_system where name = 'NotificationSender';

      v_notification_msg := v_notification_msg || c_lb || 'Import ID: ' || p_rid;

      if (v_warnings_cnt > 0) then
        set_delimiters(p_rid);
        get_csv_row(p_rid, v_start_row, v_csv_row);
        v_empty_csv_row := regexp_replace(v_csv_row, '[^,]', '');

        --pkg_email.write_text(v_cnn, v_csv_row);
        --pkg_email.write_text(v_cnn, csv_delims.fields_delimiter);
        --pkg_email.write_text(v_cnn, c_csv_err_header);
        --pkg_email.write_text(v_cnn, csv_delims.records_delimiter);
        v_csv_row := v_csv_row || csv_delims.fields_delimiter || c_csv_err_header || csv_delims.records_delimiter;
        v_csv := v_csv_row;

        for rec in (
        select e.error_msg, e.row_num, et.imp_error_type
        from imp_run_error e join imp_error_type et on (et.imp_error_type_id = e.imp_error_type_id)
        where imp_run_id = p_rid) loop
          if (rec.row_num > v_start_row) then
            get_csv_row(p_rid, rec.row_num, v_csv_row);
          else
            v_csv_row := v_empty_csv_row;
          end if;

          v_csv_row :=  v_csv_row || csv_delims.fields_delimiter;
          v_csv_row :=  v_csv_row || rec.row_num || csv_delims.fields_delimiter;
          v_csv_row :=  v_csv_row || pkg_imp_run.escape_csv_value(rec.error_msg, csv_delims, quote_rules)
                        || csv_delims.fields_delimiter;
          v_csv_row :=  v_csv_row || pkg_imp_run.escape_csv_value(rec.imp_error_type, csv_delims, quote_rules)
                        || csv_delims.fields_delimiter;
          v_csv_row :=  v_csv_row || csv_delims.records_delimiter;

          v_csv := v_csv || v_csv_row;
        end loop;
      end if;

      dbms_lob.converttoblob(b_csv, v_csv, DBMS_LOB.getlength(v_csv), dest_offset, src_offset, csid, lang_context, convert_warn);
      insert into blob_data(filename, blob_data, blob_data_len)
      values('imp_' || p_rid || '_log.csv', b_csv, DBMS_LOB.getlength(b_csv))
      returning blob_data_id into blobId;

      pkg_notif.send_notification(
          p_subject => v_notification_subj,
          p_message => v_notification_msg,
          p_sender => v_sender,
          p_to_address => v_email,
          p_attach_blob_data_id1 => blobId);
    end notify;


  procedure set_delimiters(p_rid in imp_run.imp_run_id%type) is
    v_field_delim v_delimiters.delimiter%type;
    v_line_delim v_line_delimiters.delimiter%type;
    v_quote v_string_quote.quote%type;
    begin
      select fd.delimiter, ld.delimiter, q.quote
      into v_field_delim, v_line_delim, v_quote
      from imp_run r join imp_spec s on (s.imp_spec_id = r.imp_spec_id)
        join v_delimiters fd on (fd.delimiter_id = s.field_delimiter_id)
        join v_line_delimiters ld on (ld.line_delimiter_id = s.line_delimiter_id)
        join v_string_quote q on (q.string_quote_id = s.string_quote_id)
      where r.imp_run_id = p_rid;

      csv_delims.fields_delimiter := pkg_imp_run.decode_delim_str(v_field_delim);
      csv_delims.records_delimiter := pkg_imp_run.decode_delim_str(v_line_delim);

      quote_rules.quote_symbol := pkg_imp_run.decode_delim_str(v_quote);
      quote_rules.quote_strings := 0;
      quote_rules.quote_everything := 0;
      quote_rules.quote_nulls := 0;

    end set_delimiters;


  procedure del_empty_grid_rows(
    p_rid in imp_run.imp_run_id%type,
    p_col_nums_to_skip in varchar2 default null)
  is
    v_row_num number;
    begin
      loop
        select row_num into v_row_num from (
          select row_num, count(col_num) empt_cols_cnt from imp_run_grid
          where imp_run_id = p_rid and data is null and col_num not in (
            select * from the (select cast(pkg_str.split_str2num(p_col_nums_to_skip) as tableOfNum) from dual))
          group by row_num)
        where empt_cols_cnt = (
          select max(col_num) from imp_run_grid
          where imp_run_id = p_rid and col_num not in (
            select * from the (select cast(pkg_str.split_str2num(p_col_nums_to_skip) as tableOfNum) from dual)))
              and rownum = 1;

        delete from imp_run_grid_incr where imp_run_id = p_rid and row_num = v_row_num;
        update imp_run_grid_incr set row_num = row_num - 1 where imp_run_id = p_rid and row_num > v_row_num;
      end loop;
      exception
      when no_data_found then
      --all empty rows deleted
      null;
    end del_empty_grid_rows;


  function split_str(arg_str in varchar2, arg_delim in varchar2)
    return t_str_arr
  is
    i            pls_integer;
    delim_length pls_integer;
    s_pos        pls_integer;
    f_pos        pls_integer;
    str_elem     varchar2(4000);
    source_str   varchar2(32767);

    str_arr      t_str_arr      := t_str_arr(null);
    begin
      i := 1;
      s_pos := 1;
      delim_length := Length(arg_delim);

      if (length(arg_str) > 0) and(arg_str is not null)
      then
        source_str := arg_str || arg_delim;
      else
        source_str := '';
      end if;

      loop
        f_pos := instr(source_str, arg_delim, s_pos);
        exit when (f_pos = 0) or(f_pos is null);
        str_elem := substr(source_str, s_pos, f_pos - s_pos);
        str_arr(i) := str_elem;
        str_arr.extend();
        i := i + 1;
        s_pos := f_pos + delim_length;
      end loop;

      str_arr.trim();
      return str_arr;
    end split_str;


  function decode_delim_str(arg_delim in varchar2)
      return varchar2
  is
      delims_arr t_str_arr;
      delim_str varchar2(50);
      delim varchar2(5);
      v_i pls_integer;
  begin
      delims_arr := split_str(arg_delim, '][');

      v_i := delims_arr.first;

      while v_i is not null loop
          delim := ltrim(delims_arr(v_i), '[');
          delim := rtrim(delim, ']');
          delim_str := delim_str || chr(delim);
          v_i := delims_arr.next(v_i);
      end loop;

      return delim_str;
  end decode_delim_str;


  function escape_csv_value(
    value in varchar2,
    delimiters in t_delimiters,
    quote_rules in t_quote_rules,
    quote_value in boolean default false)
    return varchar2
  is
    escaped_value varchar2(32767);
    begin
      escaped_value := value;
      escaped_value := replace(escaped_value, quote_rules.quote_symbol,
                               quote_rules.quote_symbol || quote_rules.quote_symbol);
      if   (quote_value)
           or (quote_rules.quote_everything = 1)
           or ((escaped_value is null) and (quote_rules.quote_nulls = 1))
           or (instr(escaped_value, delimiters.fields_delimiter) <> 0)
           or (instr(escaped_value, delimiters.records_delimiter) <> 0)
           or (substr(escaped_value, 1, 1) = ' ')
           or (substr(escaped_value, length(escaped_value), 1) = ' ')
           or ((delimiters.records_delimiter = chr(13) || chr(10))
               and (instr(escaped_value, chr(10)) <> 0)) then
        escaped_value := quote_rules.quote_symbol || escaped_value ||
                         quote_rules.quote_symbol;
      end if;

      return escaped_value;
    end escape_csv_value;

  --File operations restricted in AWS environment
  --    procedure loader_create_fill_ext_table(
  --        p_ext_table_name in varchar2,
  --        p_csv_file_name_lb_fixed in varchar2,
  --        p_fields_delimiter in varchar2,
  --        p_quote_symbol in varchar2)
  --    as
  --        v_create_ext_table_sql varchar2(4000);
  --    begin
  --        v_create_ext_table_sql := '
  --            create table ' || p_ext_table_name || ' (col01 varchar2(4000 byte), col02 varchar2(4000 byte), col03 varchar2(4000 byte),
  --              col04 varchar2(4000 byte), col05 varchar2(4000 byte), col06 varchar2(4000 byte), col07 varchar2(4000 byte),
  --              col08 varchar2(4000 byte), col09 varchar2(4000 byte), col10 varchar2(4000 byte), col11 varchar2(4000 byte),
  --              col12 varchar2(4000 byte), col13 varchar2(4000 byte), col14 varchar2(4000 byte), col15 varchar2(4000 byte),
  --              col16 varchar2(4000 byte), col17 varchar2(4000 byte), col18 varchar2(4000 byte), col19 varchar2(4000 byte),
  --              col20 varchar2(4000 byte), col21 varchar2(4000 byte), col22 varchar2(4000 byte), col23 varchar2(4000 byte),
  --              col24 varchar2(4000 byte), col25 varchar2(4000 byte), col26 varchar2(4000 byte), col27 varchar2(4000 byte),
  --              col28 varchar2(4000 byte), col29 varchar2(4000 byte), col30 varchar2(4000 byte)
  --            )
  --            organization external (
  --                type oracle_loader
  --                default directory ' || c_loader_ora_dir || '
  --                access parameters (
  --                    records delimited by newline
  --                    badfile ' || c_loader_ora_dir || ':''cvsempxt%a_%p.bad''
  --                    logfile ' || c_loader_ora_dir || ':''csvempxt%a_%p.log''
  --                    fields terminated by ''' || p_fields_delimiter || '''
  --                    optionally enclosed by ''' || p_quote_symbol || ''' and ''' || p_quote_symbol || '''
  --                    lrtrim
  --                    missing field values are null
  --                )
  --                location (' || c_loader_ora_dir || ':''' || p_csv_file_name_lb_fixed || ''')
  --            )
  --            reject limit 0
  --            noparallel
  --            nomonitoring';
  --        execute immediate v_create_ext_table_sql;
  --    end loader_create_fill_ext_table;
  --
  --
  --    procedure loader_copy_to_imp_run_grid(p_rid imp_run.imp_run_id%type, p_ext_table_name in varchar2)
  --    as
  --        v_fill_imp_run_grid_sql varchar2(5000);
  --    begin
  --        v_fill_imp_run_grid_sql := '
  --            insert all
  --            when col01 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,1,replace(col01,''@@@@'',chr(10)))
  --            when col02 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,2,replace(col02,''@@@@'',chr(10)))
  --            when col03 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,3,replace(col03,''@@@@'',chr(10)))
  --            when col04 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,4,replace(col04,''@@@@'',chr(10)))
  --            when col05 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,5,replace(col05,''@@@@'',chr(10)))
  --            when col06 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,6,replace(col06,''@@@@'',chr(10)))
  --            when col07 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,7,replace(col07,''@@@@'',chr(10)))
  --            when col08 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,8,replace(col08,''@@@@'',chr(10)))
  --            when col09 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,9,replace(col09,''@@@@'',chr(10)))
  --            when col10 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,10,replace(col10,''@@@@'',chr(10)))
  --            when col11 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,11,replace(col11,''@@@@'',chr(10)))
  --            when col12 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,12,replace(col12,''@@@@'',chr(10)))
  --            when col13 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,13,replace(col13,''@@@@'',chr(10)))
  --            when col14 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,14,replace(col14,''@@@@'',chr(10)))
  --            when col15 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,15,replace(col15,''@@@@'',chr(10)))
  --            when col16 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,16,replace(col16,''@@@@'',chr(10)))
  --            when col17 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,17,replace(col17,''@@@@'',chr(10)))
  --            when col18 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,18,replace(col18,''@@@@'',chr(10)))
  --            when col19 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,19,replace(col19,''@@@@'',chr(10)))
  --            when col20 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,20,replace(col20,''@@@@'',chr(10)))
  --            when col21 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,21,replace(col21,''@@@@'',chr(10)))
  --            when col22 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,22,replace(col22,''@@@@'',chr(10)))
  --            when col23 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,23,replace(col23,''@@@@'',chr(10)))
  --            when col24 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,24,replace(col24,''@@@@'',chr(10)))
  --            when col25 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,25,replace(col25,''@@@@'',chr(10)))
  --            when col26 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,26,replace(col26,''@@@@'',chr(10)))
  --            when col27 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,27,replace(col27,''@@@@'',chr(10)))
  --            when col28 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,28,replace(col28,''@@@@'',chr(10)))
  --            when col29 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,29,replace(col29,''@@@@'',chr(10)))
  --            when col30 is not null then into imp_run_grid (imp_run_id,row_num,col_num,data) values(' || p_rid || ',rn,30,replace(col30,''@@@@'',chr(10)))
  --            select t.*, rownum rn from ' || p_ext_table_name || ' t';
  --
  --        execute immediate v_fill_imp_run_grid_sql;
  --    end loader_copy_to_imp_run_grid;
  --
  --    function loader_fill_grid(
  --        p_rid imp_run.imp_run_id%type,
  --        p_update_imp_status in boolean default true)
  --        return number
  --    is
  --        v_csv_file_name varchar2(50) := 'imp_' || p_rid || '.csv';
  --        v_csv_file_name_lb_fixed varchar2(50) := 'imp_' || p_rid || '_lbfixed.csv';
  --        v_ext_table_name varchar2(30) := 'imp_loader_' || p_rid;
  --        v_imp_data clob;
  --        v_rec_cnt integer := 0;
  --        v_lbfix_job_running integer;
  --        v_keep_sql_loader_ext_table imp_spec.keep_sql_loader_ext_table%type;
  --    begin
  --        if p_update_imp_status then
  --            set_status(p_rid, c_s_parse);
  --        end if;
  --
  --        select data into v_imp_data from imp_run where imp_run_id = p_rid;
  --        dbms_xslprocessor.clob2file(v_imp_data, c_loader_ora_dir, v_csv_file_name);
  --
  --        --source file
  --        dbms_scheduler.set_job_argument_value(
  --            job_name             => c_loader_lbfix_owner || '.' || c_loader_lbfix_job_name,
  --            argument_position    => 1,
  --            argument_value       => c_loader_physical_dir || '/' || v_csv_file_name);
  --
  --        --dest file
  --        dbms_scheduler.set_job_argument_value(
  --            job_name             => c_loader_lbfix_owner || '.' || c_loader_lbfix_job_name,
  --            argument_position    => 2,
  --            argument_value       => c_loader_physical_dir || '/' || v_csv_file_name_lb_fixed);
  --
  --        dbms_scheduler.run_job(c_loader_lbfix_owner || '.' || c_loader_lbfix_job_name, true);
  --
  --        loader_create_fill_ext_table(v_ext_table_name, v_csv_file_name_lb_fixed,
  --            csv_delims.fields_delimiter, quote_rules.quote_symbol);
  --
  --        select s.keep_sql_loader_ext_table into v_keep_sql_loader_ext_table
  --        from imp_spec s join imp_run r on (r.imp_spec_id = s.imp_spec_id)
  --        where r.imp_run_id = p_rid;
  --
  --        if (v_keep_sql_loader_ext_table <> 1) then
  --            loader_copy_to_imp_run_grid(p_rid, v_ext_table_name);
  --            execute immediate 'drop table ' || v_ext_table_name;
  --            select count(*) into v_rec_cnt from imp_run_grid where imp_run_id = p_rid;
  --        end if;
  --
  --        if p_update_imp_status then
  --            set_grid_count(p_rid);
  --            set_fill_grid_finish_ts(p_rid);
  --        end if;
  --
  --        return v_rec_cnt;
  --    end loader_fill_grid;

end pkg_imp_run;
/

