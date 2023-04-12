CREATE OR REPLACE PACKAGE BODY PKG_IMP_UTILS 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

  function is_incremental return number
  is
  begin
    return v_is_incremental;
  end is_incremental;

  function is_empty_str(p_value in varchar2) return boolean is
    begin
      if ((p_value is not null) and(length(trim(p_value)) <> 0)) then
        return false;
      end if;
      return true;
    end is_empty_str;


  function get_uniqby_xtid(p_xtid in xitor_type.xitor_type_id%type)
    return xitor_type.xitor_type_id%type as

    v_unique_by_xt_id relation_type.unique_by_xt_id%type;
    v_xt xitor_type.xitor_type%type;
    v_uniq_found boolean := false;
    begin
      for rec in (select unique_by_xt_id from relation_type
      where child_type_id = p_xtid) loop

        if not(v_uniq_found) then
          v_unique_by_xt_id := rec.unique_by_xt_id;
          v_uniq_found := true;
        elsif (v_unique_by_xt_id <> rec.unique_by_xt_id)
              or ((v_unique_by_xt_id is null) and (rec.unique_by_xt_id is not null)) then
          select xitor_type into v_xt from xitor_type
          where xitor_type_id = p_xtid;

          raise_application_error(-20000, pkg_label.format(17687, pkg_label.list_label_params('trackor_type' =>  v_xt)));
        end if;
      end loop;

      if (v_unique_by_xt_id is null) then
        v_unique_by_xt_id := p_xtid;
      end if;

      return v_unique_by_xt_id;
    end get_uniqby_xtid;

  function create_def_import(
    p_rtid in relation_type.relation_type_id%type,
    p_name in imp_spec.name%type)
    return imp_spec.imp_spec_id%type as

    c_crlf constant varchar2(2) := chr(13) || chr(10);
    c_imp_desc constant varchar2(50) := 'This import was generated automatically';
    c_from_tmpl constant varchar2(50) := 'xitor x[i], ancestor a[i],' || c_crlf;
    c_where1_tmpl constant varchar2(50) :=
    'and x[i].xitor_type_id = ';
    c_where2_tmpl constant varchar2(150) :=
    'and a[i].child_id = x[i-1].xitor_id' || c_crlf ||
    'and a[i].parent_id = x[i].xitor_id' || c_crlf;
    c_where3_tmpl constant varchar2(50) :=
    'and x[i].xitor_key = '':p_key[i]''' || c_crlf;
    c_where4_tmpl constant varchar2(50) :=
    'and x[i].program_id = :p_pid' || c_crlf;

    v_relation_imp_dtid imp_data_type.imp_data_type_id%type;
    v_parent_dt_param imp_data_type_param.imp_data_type_param_id%type;
    v_del_rel_dt_param imp_data_type_param.imp_data_type_param_id%type;
    v_spec_id imp_spec.imp_spec_id%type;
    v_xtid xitor_type.xitor_type_id%type;
    v_xt xitor_type.xitor_type%type;
    v_is_autokey xitor_type.is_autokey%type;
    v_unique_by_xt_id relation_type.unique_by_xt_id%type;
    v_imp_col_id imp_column.imp_column_id%type;
    v_search_sql varchar2(4000);
    v_entity_id imp_entity.imp_entity_id%type;
    v_dmid imp_data_map.imp_data_map_id%type;
    i integer := 1;
    v_from varchar2(1000) := 'from ';
    v_where1 varchar2(1000);
    v_where2 varchar2(1000);
    v_where3 varchar2(1000);
    v_where4 varchar2(1000);

    function replace_idx(p_str in varchar2, p_idx in integer)
      return varchar2 as
      v_result varchar2(150);
      begin
        v_result := replace(p_str, '[i]', p_idx);
        v_result := replace(v_result, '[i-1]', p_idx - 1);
        return v_result;
      end;
    begin
      select imp_data_type_id into v_relation_imp_dtid
      from imp_data_type where upper(name) = 'RELATION';

      select imp_data_type_param_id into v_parent_dt_param
      from imp_data_type_param
      where upper(sql_parameter) = 'PARENT_XITOR_TYPE_ID' and
            imp_data_type_id = v_relation_imp_dtid;

      select imp_data_type_param_id into v_del_rel_dt_param
      from imp_data_type_param
      where upper(sql_parameter) = 'DEL_EXIST_REL' and
            imp_data_type_id = v_relation_imp_dtid;


      -- Create imp spec
      insert into imp_spec(imp_file_type_id, name, description, date_format, time_format,
                           external_proc, line_delimiter_id, field_delimiter_id, string_quote_id, imp_action)
      values(1, p_name, c_imp_desc, 'MM/DD/YYYY', 'HH24:MI:SS',
             'pkg_ext_imp.XitorConfiguredFieldLoad(:rid);', 1000061, 1000061, 1000061, '2,3,4')
      returning imp_spec_id into v_spec_id;

      select child_type_id, v_unique_by_xt_id into v_xtid, v_unique_by_xt_id
      from relation_type where relation_type_id = p_rtid;

      -- Create entity
      insert into imp_entity(imp_spec_id, sql_text, order_number, xitor_type_id,
                             entity_name)
      values(v_spec_id, 'tmp', 1, v_xtid,
             (select xitor_type from xitor_type where xitor_type_id = v_xtid))
      returning imp_entity_id into v_entity_id;

      select is_autokey into v_is_autokey
      from xitor_type where xitor_type_id = v_xtid;

      -- Fill entity req fields

      update imp_entity_req_field set value = 'PROGRAM_ID'
      where imp_entity_id = v_entity_id and xitor_req_field_id = (
        select xitor_req_field_id from xitor_req_field
        where xitor_type_id = v_xtid and field_name = 'PROGRAM_ID');

      update imp_entity_req_field set value = 'ZONE_ID'
      where imp_entity_id = v_entity_id and xitor_req_field_id = (
        select xitor_req_field_id from xitor_req_field
        where xitor_type_id = v_xtid and field_name = 'ZONE_ID');

      loop
        v_unique_by_xt_id := get_uniqby_xtid(v_xtid);

        select xitor_type, is_autokey
        into v_xt, v_is_autokey
        from xitor_type where xitor_type_id = v_xtid;

        insert into imp_column(imp_spec_id, order_number, name)
        values(v_spec_id, i, v_xt || '_XITOR_KEY')
        returning imp_column_id into v_imp_col_id;

        insert into imp_entity_param(imp_entity_id, sql_parameter, imp_column_id)
        values(v_entity_id, 'p_key' || i, v_imp_col_id);

        v_where1 := v_where1 || replace_idx(c_where1_tmpl, i) || v_xtid || c_crlf;
        v_where3 := v_where3 || replace_idx(c_where3_tmpl, i);

        if (i = 1) then
          v_from := v_from || 'xitor x' || i || ',' || c_crlf;

          if (v_is_autokey <> 1) then
            update imp_entity_req_field set imp_column_id = v_imp_col_id
            where imp_entity_id = v_entity_id and xitor_req_field_id = (
              select xitor_req_field_id from xitor_req_field
              where xitor_type_id = v_xtid and field_name = 'TRACKOR_KEY');
          end if;
        else
          v_from := v_from || replace_idx(c_from_tmpl, i);
          v_where2 := v_where2 || replace_idx(c_where2_tmpl, i);
        end if;

        if (i = 2) then
          -- Create Mapping
          insert into imp_data_map(imp_spec_id, imp_data_type_id,
                                   imp_entity_id, imp_column_id, data_map_name)
          values(v_spec_id, v_relation_imp_dtid, v_entity_id, v_imp_col_id,
                 (select name from imp_column where imp_column_id = v_imp_col_id))
          returning imp_data_map_id into v_dmid;

          insert into imp_data_type_param_value(
            imp_data_map_id, imp_data_type_param_id, value)
          values(v_dmid, v_del_rel_dt_param, 'Yes');

          insert into imp_data_type_param_value(
            imp_data_map_id, imp_data_type_param_id, value)
          values(v_dmid, v_parent_dt_param, v_xtid);
        end if;

        v_where4 := v_where4 || replace_idx(c_where4_tmpl, i);

        exit when (v_unique_by_xt_id = v_xtid);
        v_xtid := v_unique_by_xt_id;
        i := i + 1;
      end loop;

      v_search_sql := 'select distinct x1.xitor_id ' || c_crlf ||
                      substr(v_from, 1, length(v_from) - 3) || c_crlf ||
                      'where' || c_crlf || substr(v_where1, 5, length(v_where1)) ||
                      v_where2 || v_where3 ||
                      substr(v_where4, 1, length(v_where4) - 2) || ';';

      update imp_entity set sql_text = v_search_sql
      where imp_entity_id = v_entity_id;

      if (instr(v_search_sql, ':p_pid') <> 0) then
        insert into imp_entity_param(imp_entity_id, sql_parameter, parameter_value)
        values(v_entity_id, 'p_pid', 'PROGRAM_ID');
      end if;

      return v_spec_id;
    end create_def_import;


    procedure prepare_imp_delta(p_rid imp_run.imp_run_id%type)
    as
        v_last_run_rid imp_run.imp_run_id%type;
        v_imp_spec_id imp_spec.imp_spec_id%type;
        v_pid imp_spec.program_id%type;
        v_current_start_row imp_run.start_row%type;
        v_last_start_row imp_run.start_row%type;
    begin
        -- 0) Find last run of this import
        select imp_spec_id, program_id, start_row
          into v_imp_spec_id, v_pid, v_current_start_row
          from imp_run
         where imp_run_id = p_rid;

        begin
            --Only finished processes with status:
            --Executed without Warnings,
            --Executed with Warnings,
            --Recovered,
            --Interrupted
            select imp_run_id, start_row
              into v_last_run_rid, v_last_start_row
              from (select ir.imp_run_id, ir.start_row
                      from imp_run ir 
                      join process p on p.process_id = ir.process_id
                     where ir.imp_spec_id = v_imp_spec_id
                       and ir.program_id = v_pid
                       and ir.imp_run_id <> p_rid
                       and p.status_id in (6, 7, 12, 14)
                       and p.end_date is not null
                     order by p.end_date desc)
             where rownum = 1;
        exception 
            when no_data_found then
                v_last_run_rid := 0;
                v_last_start_row := 1;
        end;

        -- Find File Columns used to find Primary Keys for all Entities.
        insert into imp_run_pk_col (col_name,new_col,old_col)
        select distinct c.name,
               nirg.COL_NUM,
               oirg.COL_NUM
          from imp_spec spec
          join imp_entity ent on (ent.imp_spec_id = spec.imp_spec_id)
          join imp_entity_param p on (p.IMP_ENTITY_ID = ent.IMP_ENTITY_ID and p.imp_column_id is not null)
          join imp_column c on (c.imp_column_id = p.imp_column_id)
          join imp_run_grid_incr nirg on (nirg.imp_run_id = p_rid and nirg.row_num = v_current_start_row and nirg.data = c.name)
          join imp_run_grid_incr oirg on (oirg.imp_run_id = v_last_run_rid and oirg.row_num = v_last_start_row and oirg.data = c.name)
         where spec.imp_spec_id = v_imp_spec_id;

        -- 1) Match rows based on number of given PK Columns
        --     a) If multiples, take last row
        insert into imp_run_row_match(new_row, old_row)
        select nirg.row_num new_row, oirg.row_num old_row
        from (
          select row_num, listagg(data, ',' on overflow truncate without count) within group (order by col_num) pk
          from imp_run_grid_incr
          where imp_run_id = p_rid and row_num > v_current_start_row and col_num in (select new_col from imp_run_pk_col)
          group by imp_run_id, row_num
        ) nirg
        left outer join (
          select row_num, listagg(data, ',' on overflow truncate without count) within group (order by col_num) pk
          from imp_run_grid_incr
          where imp_run_id = v_last_run_rid and row_num > v_last_start_row and col_num in (select old_col from imp_run_pk_col)
          group by imp_run_id, row_num
        ) oirg on (oirg.pk = nirg.pk);

        -- 2) Match columns based on first row given
        insert into imp_run_col_match(new_col, old_col)
        select nirg.col_num as new_col, oirg.col_num as old_col
        from imp_run_grid_incr nirg left outer join imp_run_grid_incr oirg
        on (oirg.imp_run_id = v_last_run_rid and oirg.row_num = v_last_start_row and oirg.data = nirg.data)
        where nirg.imp_run_id = p_rid and nirg.row_num = v_current_start_row;

        -- 3) Find grid deltas from grids based on newly matched rows and columns
        --TODO using clob_data with "MINUS" causes "ORA-00932: inconsistent datatypes: expected - got CLOB"
        --need to manually compare clob values in loop and fill imp_run_grid_deltas
        insert into imp_run_grid_deltas(row_num, col_num, data)
        select row_num, col_num, data
        from imp_run_grid_incr
        where imp_run_id = p_rid and col_num not in (select new_col from imp_run_pk_col) and row_num > v_current_start_row
        minus
        select rm.new_row as row_num, cm.new_col as col_num, irg.data
        from imp_run_grid_incr irg
        join imp_run_row_match rm on (rm.old_row = irg.row_num)
        join imp_run_col_match cm on (cm.old_col = irg.col_num)
        where imp_run_id = v_last_run_rid;

        -- 5) Fill imp_run_grid
        --   a) Header Row
        merge into imp_run_grid_incr dirg
        using (
          select imp_run_id, row_num, col_num, data, clob_data, 1, v_pid as program_id
          from imp_run_grid_incr
          where imp_run_id = p_rid and row_num = v_current_start_row
          and (col_num in (select new_col from imp_run_pk_col) or col_num in (select col_num from imp_run_grid_deltas))
          ) nirg
        on (
          nirg.program_id = dirg.program_id
          and nirg.imp_run_id = dirg.imp_run_id
          and nirg.row_num = dirg.row_num
          and nirg.col_num = dirg.col_num
          )
        when matched then
          update set is_delta_val = 1;
        /*
        insert into imp_run_grid_incr(imp_run_id, row_num, col_num, data, clob_data, is_delta_val, program_id)
        select p_rid, row_num, col_num, data, clob_data, 1, v_pid
        from imp_run_grid_incr
        where imp_run_id = p_rid and row_num = 0
        and (col_num in (select new_col from imp_run_pk_col) or col_num in (select col_num from imp_run_grid_deltas));
        */

        --   b) PK Columns
        merge into imp_run_grid_incr dirg
        using (
          select imp_run_id, row_num, col_num, data, clob_data, 1, v_pid as program_id
          from imp_run_grid_incr
          where imp_run_id = p_rid and col_num in(select new_col from imp_run_pk_col)
          and row_num in (select row_num from imp_run_grid_deltas)
          ) nirg
        on (
          nirg.program_id = dirg.program_id
          and nirg.imp_run_id = dirg.imp_run_id
          and nirg.row_num = dirg.row_num
          and nirg.col_num = dirg.col_num
          )
        when matched then
          update set is_delta_val = 1;
        /*
        insert into imp_run_grid_incr(imp_run_id, row_num, col_num, data, clob_data, is_delta_val, program_id)
        select p_rid, row_num, col_num, data, clob_data, 1, v_pid
        from imp_run_grid_incr
        where imp_run_id = p_rid and col_num in(select new_col from imp_run_pk_col)
        and row_num in (select row_num from imp_run_grid_deltas);
        */

        --     c) Deltas from temp table
        merge into imp_run_grid_incr dirg
        using (
          select p_rid as imp_run_id, row_num, col_num, data, clob_data, 1, v_pid as program_id
          from imp_run_grid_deltas
          ) nirg
        on (
          nirg.program_id = dirg.program_id
          and nirg.imp_run_id = dirg.imp_run_id
          and nirg.row_num = dirg.row_num
          and nirg.col_num = dirg.col_num
          )
        when matched then
          update set is_delta_val = 1;
        /*
        insert into imp_run_grid_incr(imp_run_id, row_num, col_num, data, clob_data, is_delta_val, program_id)
        select p_rid, row_num, col_num, data, clob_data, 1, v_pid
        from imp_run_grid_deltas;
        */

        -- 6) clenaup
        delete from imp_run_row_match;
        delete from imp_run_col_match;
        delete from imp_run_grid_deltas;

    end prepare_imp_delta;


    procedure set_entity_pk_count(p_rid imp_run.imp_run_id%type) as
        pragma autonomous_transaction;
    begin
        update imp_run
        set entity_pk_count = (select count(*) from imp_run_entity_pk where imp_run_id = p_rid and pk is not null)
        where imp_run_id = p_rid;

        commit;
    end set_entity_pk_count;


    procedure set_fill_entity_pks_finish_ts(p_rid imp_run.imp_run_id%type) as
        pragma autonomous_transaction;
    begin
        update imp_run set search_pk_finished_ts = current_date
        where imp_run_id = p_rid;

        commit;
    end set_fill_entity_pks_finish_ts;

    function get_col_num(
        p_rid imp_run.imp_run_id%type,
        p_colname imp_column.name%type)
        return imp_run_grid_incr.col_num%type 
    as
        v_colnum number;
    begin
        select col_num into v_colnum 
        from imp_run_grid irg
        join imp_run ir on irg.imp_run_id = ir.imp_run_id
        where irg.imp_run_id = p_rid and irg.row_num = ir.start_row and upper(irg.data) = upper(p_colname);

        return v_colnum;
    exception
        when no_data_found then
            return null;
    end get_col_num;

    function get_col_data(
        p_rid imp_run.imp_run_id%type,
        p_colname imp_column.name%type,
        p_row_num imp_run_grid_incr.row_num%type)
    return imp_run_grid_incr.data%type
    as
        v_result imp_run_grid_incr.data%type;
    begin
        select data into v_result
          from imp_run_grid
         where imp_run_id = p_rid and data is not null
           and row_num = p_row_num
           and col_num = (
                select col_num 
                  from imp_run_grid
                 where imp_run_id = p_rid 
                   and upper(data) = upper(p_colname)
                   and row_num = (select start_row
                                    from imp_run
                                   where imp_run_id = p_rid));

        return v_result;
    exception
        when no_data_found then
            return null;
    end get_col_data;


    function convert_boolean(p_bool_value in varchar2) return number is
        v_result number;
    begin
        if ((p_bool_value is null) or (p_bool_value = 'NULL') or (length(trim(p_bool_value)) = 0)) then
            v_result := 0;
        elsif upper(trim(p_bool_value)) in ('Y', 'YES', '1', 'ALL', 'TRUE') then
            v_result := 1;
        else
            v_result := 0;
        end if;

        return v_result;
    end convert_boolean;

    function get_col_name(
        p_rid in imp_run.imp_run_id%type,
        p_col_num in imp_run_grid_incr.col_num%type) return varchar2
    as
        v_col_name imp_run_grid.data%type;
    begin
        select upper(data)
          into v_col_name
          from imp_run_grid
         where imp_run_id = p_rid 
           and col_num = p_col_num
           and row_num = (select start_row
                            from imp_run
                           where imp_run_id = p_rid);

        return v_col_name;
    exception
        when no_data_found then
            return null;            
    end get_col_name;
end pkg_imp_utils;
/