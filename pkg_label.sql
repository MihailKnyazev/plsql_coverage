CREATE OR REPLACE PACKAGE BODY PKG_LABEL 
as
---------------------------------------------------------------
-- Private declarations
---------------------------------------------------------------

    function get_not_found_label(
      p_label_id label_system.label_system_id%type,
      p_lang_id app_languages.app_lang_id%type) 
    return varchar2;

    function get_field_prefix(
        p_trackor_type_id number,
        p_lang_id number)
    return varchar2;

    function get_trackor_type_param(
        p_trackor_type_id number,
        p_field_name varchar2)
    return number;

    procedure set_all_languages_ts;

    procedure allow_table_modify;
    procedure prohibit_table_modify;

---------------------------------------------------------------
-- Public functions implementation
---------------------------------------------------------------
  procedure no_direct_table_mods as
  begin
    if not allowtablemods then
      raise_application_error (-20000, 'Do not modify the table directly.  Use PKG_LABEL instead.');
    end if;
  end no_direct_table_mods;

  procedure copy_program_labels_to_lang(
      p_source_lang_id app_languages.app_lang_id%type,
      p_target_lang_id app_languages.app_lang_id%type,
      p_pid program.program_id%type) as
  begin
      allow_table_modify;

      merge into label_program lp
      using (
        select 
          p_target_lang_id as app_lang_id,
          label_program_id,
          label_program_text,
          program_id 
        from label_program
        where app_lang_id = p_source_lang_id
          and program_id = p_pid
      ) d on (d.app_lang_id = lp.app_lang_id and d.label_program_id = lp.label_program_id)
      when matched then update set lp.label_program_text = d.label_program_text
      when not matched then 
        insert(app_lang_id, label_program_id, label_program_text, program_id)
        values(d.app_lang_id, d.label_program_id, d.label_program_text, d.program_id);

      prohibit_table_modify;
      set_app_languages_ts(p_target_lang_id);
  end copy_program_labels_to_lang;

  function copy_label_system_to_program(p_label_id in label_system.label_system_id%type,
      source_pid in program.program_id%type)
      return label_program.label_program_id%type 
  is
      v_app_label_id label_program.label_program_id%type;
  begin
      if source_pid = 0 then
          raise_application_error(-20011, 'Progam Labels cannot be created in Zero Program!'); 
      end if;

      select seq_label_program_id.nextval into v_app_label_id from dual;

      allow_table_modify;
      insert into label_program (
        app_lang_id,
        label_program_id,
        label_program_text,
        program_id
      ) select
        app_lang_id,
        v_app_label_id,
        label_system_text,
        source_pid
      from label_system
      where label_system_id = p_label_id;
      prohibit_table_modify;

      set_all_languages_ts;
      return v_app_label_id;
  end copy_label_system_to_program;

  function copy_label_program(p_label_id in label_program.label_program_id%type)
      return label_program.label_program_id%type is
      v_app_label_id label_program.label_program_id%type;
  begin
      select seq_label_program_id.nextval into v_app_label_id from dual;

      allow_table_modify;
      insert into label_program (
        app_lang_id,
        label_program_id,
        label_program_text,
        program_id
      ) select
        app_lang_id,
        v_app_label_id,
        label_program_text,
        program_id
      from label_program
      where label_program_id = p_label_id
        and program_id <> 0;
      prohibit_table_modify;

      set_all_languages_ts;
      return v_app_label_id;
  end copy_label_program;

  function copy_label_program(
        p_label_id in label_program.label_program_id%type,
        source_pid in program.program_id%type
  ) return label_program.label_program_id%type is
      v_app_label_id label_program.label_program_id%type;
  begin
      if source_pid = 0 then
          raise_application_error(-20011, 'Progam Labels cannot be created in Zero Program!'); 
      end if;

      select seq_label_program_id.nextval into v_app_label_id from dual;

      allow_table_modify;
      insert into label_program (
        app_lang_id,
        label_program_id,
        label_program_text,
        program_id
      ) select
        app_lang_id,
        v_app_label_id,
        label_program_text,
        source_pid
      from label_program
      where label_program_id = p_label_id;
      prohibit_table_modify;

      set_all_languages_ts;
      return v_app_label_id;
  end copy_label_program;

  function create_label_program(p_label in label_program.label_program_text%type)
      return label_program.label_program_id%type is
      v_app_label_id label_program.label_program_id%type;
  begin
      v_app_label_id := create_label_program(p_label, pkg_sec.get_pid);
      return v_app_label_id;
  end create_label_program;

  function create_label_program(
    p_label in label_program.label_program_text%type,
    p_pid in program.program_id%type
  ) return label_program.label_program_id%type is
      v_app_label_id label_program.label_program_id%type;
  begin
      if p_pid = 0 then
        raise_application_error(-20011, 'Progam Labels cannot be created in Zero Program!'); 
      elsif (p_label is null) then
        raise_application_error(-20000, 'Unable create program label with empty text'); 
      end if;

      select seq_label_program_id.nextval into v_app_label_id from dual;

      allow_table_modify;
      insert into label_program(app_lang_id, label_program_id, label_program_text, program_id)
      select app_lang_id, v_app_label_id, p_label, p_pid 
        from app_languages where app_lang_id not in (98, 99);
      prohibit_table_modify;

      set_all_languages_ts;
      return v_app_label_id;
  end create_label_program;

  procedure add_label_programs(p_labels list_label, p_pid program.program_id%type)
  as
    c_def_global_lang_id app_languages.app_lang_id%type := 1;
    v_def_lang_id app_languages.app_lang_id%type;
    v_add_labels list_label := new list_label();
    v_def_label varchar2(4000 char);
  begin
      if p_pid = 0 then
          raise_application_error(-20011, 'Progam Labels cannot be created in Zero Program!'); 
      end if;

      v_def_lang_id := pkg_const.get_param_program_val('Default_Language', p_pid);
      for rec_lbl_id in (select distinct app_label_id from table(p_labels) where app_label_id is not null)
      loop
        --Get default label if it does not exist for some languages - at first we search label of 'Default_Language',
        --then we search label of APP_LANGUAGES.APP_LANG_ID = 1 and then any not empty label
        select label_text into v_def_label
        from (
          select label_text, ord_num
          from (
            select t.label_text, 0 as ord_num
            from table(p_labels) t
            where t.app_label_id = rec_lbl_id.app_label_id
              and t.app_lang_id = v_def_lang_id
            union all 
            select t.label_text, 1 as ord_num
            from table(p_labels) t
            where t.app_label_id = rec_lbl_id.app_label_id
              and t.app_lang_id = c_def_global_lang_id
            union all
            select t.label_text, 2 as ord_num
            from table(p_labels) t
            where t.app_label_id = rec_lbl_id.app_label_id
              and t.label_text is not null and trim(length(t.label_text)) > 0
          ) order by ord_num
        ) where rownum = 1;

        for rec_lang_id in (
          select l.app_lang_id
          from app_languages l
          where app_lang_id not in (98,99)
          minus
          select t.app_lang_id
          from table(p_labels) t
          where t.app_label_id = rec_lbl_id.app_label_id
            and t.app_lang_id is not null
          minus 
          select l.app_lang_id
          from label_program l
          where l.label_program_id = rec_lbl_id.app_label_id
        ) loop
          v_add_labels.extend;
          v_add_labels(v_add_labels.count) := new t_label(rec_lang_id.app_lang_id, rec_lbl_id.app_label_id, v_def_label);
        end loop;
      end loop;

      allow_table_modify;
      insert into label_program(app_lang_id, label_program_id, label_program_text, program_id)
      select
          l.app_lang_id,
          t.app_label_id,
          nvl(t.label_text, v_def_label) as label_text,
          p_pid
      from
          app_languages l
          join table(p_labels) t on (t.app_lang_id = l.app_lang_id)
          left outer join label_program el on (el.label_program_id = t.app_label_id and el.app_lang_id = t.app_lang_id and el.program_id = p_pid)
      where l.app_lang_id not in (98,99)
        and t.app_label_id is not null
        and el.label_program_id is null
      union all
      select
          t.app_lang_id,
          t.app_label_id,
          t.label_text,
          p_pid
      from table(v_add_labels) t;
      prohibit_table_modify;

      set_all_languages_ts;
  end add_label_programs;

  function create_label_programs(p_labels tableofchar, p_pid program.program_id%type)
        return tableofnum 
  as
    v_ids tableofnum;

    v_bulk_errors exception;
    pragma exception_init(v_bulk_errors, -24381);
  begin
    if p_pid = 0 then
      raise_application_error(-20011, 'Progam Labels cannot be created in Zero Program!'); 
    end if;

    select seq_label_program_id.nextval bulk collect into v_ids
    from table(p_labels);

    allow_table_modify;
    forall idx in v_ids.first..v_ids.last save exceptions
      insert into label_program(app_lang_id, label_program_id, label_program_text, program_id)
        select lang.app_lang_id, v_ids(idx), p_labels(idx), p_pid
        from app_languages lang 
        where lang.app_lang_id not in (98,99);
    prohibit_table_modify;

    set_all_languages_ts;
    return v_ids;
  exception
    when v_bulk_errors then
      raise_application_error(-20000,'<ERRORMSG>Unable to create labels</ERRORMSG>');
  end create_label_programs;

  procedure delete_label_program(p_id label_program.label_program_id%type) as
    v_rows_affected number;
  begin
    v_rows_affected := delete_label_program(p_id);
  end delete_label_program;

  function delete_label_program(p_id label_program.label_program_id%type)
        return number as
  begin
    if pkg_program.is_deleting then
      return 0;
    end if;

    allow_table_modify;
    delete from label_program where label_program_id = p_id;
    prohibit_table_modify;

    set_all_languages_ts;
    return sql%rowcount;
  end delete_label_program;

  procedure delete_label_programs(p_ids tableofnum) as
    v_rows_affected number;
  begin
    v_rows_affected := delete_label_programs(p_ids);
  end delete_label_programs;

  function delete_label_programs(p_ids tableofnum)
      return number as
  begin
    if pkg_program.is_deleting then
      return 0;
    end if;

    allow_table_modify;
    delete from label_program where label_program_id in (select column_value from table(p_ids));
    prohibit_table_modify;

    set_all_languages_ts;
    return sql%rowcount;
  end delete_label_programs;

  function get_label_system(p_label_id number, p_lang_id number)
      return label_system.label_system_text%type is
      text label_system.label_system_text%type;
      lang_id number;
  begin
    begin
      lang_id := p_lang_id;
      if Nvl(lang_id, -1) = -1 then
         lang_id := 1; --English
      end if;
      select label_system_text into text
      from label_system l
      where l.label_system_id = p_label_id
            and l.app_lang_id = lang_id;
    exception
      when others then text := get_not_found_label(p_label_id, lang_id);
    end;
    return text;
  end get_label_system;

  function get_label_program(p_label_id number, p_lang_id number)
      return label_program.label_program_text%type is

      text label_program.label_program_text%type;
      lang_id number;
  begin
    begin
      lang_id := p_lang_id;
      if Nvl(lang_id, -1) = -1 then
         lang_id := 1; --English
      end if;
      select label_program_text into text
      from label_program l
      where l.label_program_id = p_label_id
            and l.app_lang_id = lang_id;
    exception
      when others then text := get_not_found_label(p_label_id, p_lang_id);
    end;
    return text;
  end get_label_program;

  function get_label_system_program(p_label_id number, p_lang_id number)
      return label_program.label_program_text%type is

      text label_program.label_program_text%type;
      lang_id number;
  begin
    begin
      lang_id := p_lang_id;
      if Nvl(lang_id, -1) = -1 then
         lang_id := 1; --English
      end if;
      select label_text into text
      from vw_label l
      where l.label_id = p_label_id
            and l.app_lang_id = lang_id;
    exception
      when others then text := get_not_found_label(p_label_id, p_lang_id);
    end;
    return text;
  end get_label_system_program;

  function get_label_task(p_label_id number, p_lang_id number)
      return label_task.label_task_text%type is

      text label_task.label_task_text%type;
      lang_id number;
  begin
    begin
      lang_id := p_lang_id;
      if Nvl(lang_id, -1) = -1 then
         lang_id := 1; --English
      end if;
      select label_task_text into text
      from label_task l
      where l.label_task_id = p_label_id
            and l.app_lang_id = lang_id;
    exception
      when others then text := get_not_found_label(p_label_id, p_lang_id);
    end;
    return text;
  end get_label_task;

  function get_label_system(p_label_id number) return label_system.label_system_text%type
  is
      v_lang_id app_languages.app_lang_id%type;
      v_label_text label_system.label_system_text%type;
  begin
      v_lang_id := pkg_sec.get_lang();
      begin
          select label_system_text into v_label_text
          from label_system
          where label_system_id = p_label_id
                and app_lang_id = v_lang_id;
      exception
          when no_data_found then
              v_label_text := get_not_found_label(p_label_id, v_lang_id);
      end;
      return v_label_text;
  end get_label_system;

  function get_label_program(p_label_id number) return label_program.label_program_text%type
  is
      v_lang_id app_languages.app_lang_id%type;
      v_label_text label_program.label_program_text%type;
  begin
      v_lang_id := pkg_sec.get_lang;
      begin
          select label_program_text into v_label_text
          from label_program
          where label_program_id = p_label_id
                and app_lang_id = v_lang_id;
      exception
          when no_data_found then
              v_label_text := get_not_found_label(p_label_id, v_lang_id);
      end;
      return v_label_text;
  end get_label_program;

  function get_label_system_program(p_label_id number) return label_program.label_program_text%type
  is
      v_lang_id app_languages.app_lang_id%type;
      v_label_text label_program.label_program_text%type;
  begin
      v_lang_id := pkg_sec.get_lang;
      begin
          select label_text into v_label_text
          from vw_label
          where label_id = p_label_id
                and app_lang_id = v_lang_id;
      exception
          when no_data_found then
              v_label_text := get_not_found_label(p_label_id, v_lang_id);
      end;
      return v_label_text;
  end get_label_system_program;

  function get_label_task(p_label_id number) return label_task.label_task_text%type
  is
      v_lang_id app_languages.app_lang_id%type;
      v_label_text label_task.label_task_text%type;
  begin
      v_lang_id := pkg_sec.get_lang;
      begin
          select label_task_text into v_label_text
          from label_task
          where label_task_id = p_label_id
                and app_lang_id = v_lang_id;
      exception
          when no_data_found then
              v_label_text := get_not_found_label(p_label_id, v_lang_id);
      end;
      return v_label_text;
  end get_label_task;

  /*function get_attrib_label(
      langid app_labels.app_lang_id%type,
      labelid app_labels.app_label_id%type,
      attribname varchar2)
      return varchar2 is result app_labels.label_text%type;
  begin
      result:=attribname;
      select label_text into result from app_labels
      where app_lang_id=langid and app_label_id=labelid;

      return(result);
  exception
      when no_data_found then
          return(result);
  end get_attrib_label;*/

  function get_app_label_for_wf_step(
        p_label_text varchar2,
        p_ttid number,
        p_ttid_key_value_field number,
        p_lang_id number)
    return varchar2
    is
        v_result varchar2(100);
    begin
        v_result := ' ' || p_label_text;
        if p_ttid = 99 then
            v_result := ' ' || get_field_prefix(p_ttid, p_lang_id) || v_result;
        end if;
        v_result := get_field_prefix(p_ttid_key_value_field, p_lang_id) || v_result;
        return v_result;
    end get_app_label_for_wf_step;

    function get_tab_label_for_wf_step(
        p_label_text varchar2,
        p_ttid number,
        p_ttid_key_value_field number,
        p_lang_id number)
    return varchar2
    is
        v_result varchar2(100);
    begin
        v_result := ' ' || p_label_text;
        if p_ttid is not null then
            v_result := get_field_prefix(p_ttid, p_lang_id) || v_result;
        else
            v_result := get_field_prefix(p_ttid_key_value_field, p_lang_id) || v_result;
        end if;
        return v_result;
    end get_tab_label_for_wf_step;

    function get_field_prefix(
        p_trackor_type_id number,
        p_lang_id number)
    return varchar2
    is
        v_ret varchar2(4000);
    begin
        if p_trackor_type_id = 99 then
            v_ret := get_label_system(1821, p_lang_id);
        elsif p_trackor_type_id = 100 then
            v_ret :=  get_label_system(1823, p_lang_id);
        else
            if (get_trackor_type_param(p_trackor_type_id, 'PREFIX_LABEL_ID') < 0) then
                v_ret := get_label_program(get_trackor_type_param(p_trackor_type_id, 'PREFIX_LABEL_ID'), p_lang_id);
            else
                v_ret := get_label_system(get_trackor_type_param(p_trackor_type_id, 'PREFIX_LABEL_ID'), p_lang_id);
            end if;
        end if;

        return v_ret;
    end get_field_prefix;

    function get_trackor_type_param(
        p_trackor_type_id number,
        p_field_name varchar2)
    return number
    is
        v_prefix_label_id number;
        v_result number;
    begin
        select x.prefix_label_id into v_prefix_label_id
        from xitor_type x
        where x.xitor_type_id = p_trackor_type_id;

        if upper(p_field_name) = 'PREFIX_LABEL_ID' then
            v_result := v_prefix_label_id;
        end if;

        return v_result;
    end get_trackor_type_param;

    procedure set_app_languages_ts(p_app_lang_id app_languages.app_lang_id%type)
    as
        pragma autonomous_transaction;
    begin
        update app_languages set ts = current_date + 1/24/60 where app_lang_id = p_app_lang_id;
        commit;
    end set_app_languages_ts;

    procedure set_all_languages_ts as
        pragma autonomous_transaction;
    begin
        update app_languages set ts = current_date + 1/24/60 where app_lang_id not in (98, 99);
        commit;
    end set_all_languages_ts;

    function delete_lang(p_lang_id app_languages.app_lang_id%type) 
      return number as
    begin
      allow_table_modify;

      delete from app_languages where app_lang_id = p_lang_id;

      prohibit_table_modify;
      return sql%rowcount;
    end delete_lang;

    function get_label_system_or_def(
      p_label_id label_system.label_system_id%type,
      p_lang_id app_languages.app_lang_id%type,
      p_pid program.program_id%type default null) 
    return label_system.label_system_text%type as
      v_text label_system.label_system_text%type;
      v_def_lang_id app_languages.app_lang_id%type;
      v_pid program.program_id%type;
    begin
      --Search label by passed Lang ID
      begin
        select label_system_text into v_text
        from label_system 
        where label_system_id = p_label_id 
          and app_lang_id = p_lang_id;
      exception
        when no_data_found then
          v_text := null;
      end;

      if v_text is null then
        --Search label by default Lang ID
        v_pid := p_pid;
        if v_pid is null then
          v_pid := pkg_sec.get_pid();
        end if;
        v_def_lang_id := pkg_const.get_param_program_val('Default_Language', v_pid);
        begin
          select label_system_text into v_text
          from label_system 
          where label_system_id = p_label_id 
            and app_lang_id = v_def_lang_id;
        exception
          when no_data_found then
            v_text := null;
        end;
      end if;

      if v_text is null then
        --Search label by internal Lang ID
        begin
          select label_system_text into v_text
          from label_system 
          where label_system_id = p_label_id 
            and app_lang_id = c_internal_lang_id;
        exception
          when no_data_found then
            v_text := null;
        end;
      end if;

      if v_text is null then
        --Search any not blank label
        begin
          select label_system_text into v_text
          from label_system 
          where label_system_id = p_label_id 
            and label_system_text is not null
            and rownum = 1;
        exception
          when no_data_found then
            v_text := null;
        end;
      end if;

      return nvl(v_text, 'Lbl:' || to_char(p_label_id));
    end get_label_system_or_def;

    procedure allow_table_modify as
    begin
      if not pkg_program.is_deleting then
        allowtablemods := true;
      end if;
    end allow_table_modify;

    procedure prohibit_table_modify as
    begin
      if not pkg_program.is_deleting then
        allowtablemods := false;
      end if;
    end prohibit_table_modify;

    function get_not_found_label(
      p_label_id label_system.label_system_id%type,
      p_lang_id app_languages.app_lang_id%type)
    return varchar2 as
    begin
      return get_label_system_or_def(c_not_found_label_id, p_lang_id)||to_char(p_label_id);
    end get_not_found_label;

end pkg_label;
/