CREATE OR REPLACE PACKAGE BODY PKG_SEC as
  g_cu number;
  g_lang_id number;
  g_pid number;
  g_is_java number;

  procedure fill_vars(p_uid number default null);

  procedure config_session(
      p_uid users.user_id%type,
      p_pid users.program_id%type,
      p_lang_id users.app_lang_id%type,
      p_disable_audit_log number) as
  begin
      g_cu := p_uid;
      g_pid := p_pid;
      g_lang_id := p_lang_id;
      if (p_disable_audit_log = '1') then
          pkg_audit.disable_audit_log := true;
      else
          pkg_audit.disable_audit_log := false;
      end if;
      g_is_java := 1;
  end config_session;

  procedure set_cu(p_id number) as
  begin
      g_cu := p_id;
      g_lang_id := null;
      g_pid := null;
      g_is_java := null;
  end;

  function get_cu return number as
  begin
    return g_cu;
  end;

  procedure set_lang(p_id number) as
  begin
      g_lang_id := p_id;
  end;

  function get_lang return number as
      v_uid number;
      v_pid number;
  begin
      if (g_lang_id is not null) then
          return g_lang_id;
      end if;

      v_uid := get_cu();
      v_pid := get_pid();

      if (v_uid is not null) then
          g_lang_id := get_lang(v_uid);

      elsif (v_pid is not null) then
          g_lang_id := to_number(pkg_const.get_param_program_val('Default_Language', v_pid));

      else
          g_lang_id := 1;
      end if;

      return g_lang_id;
  end get_lang;

  function get_lang(p_uid in users.user_id%type) return app_languages.app_lang_id%type
  is
  begin
      if (g_lang_id is null) then
          fill_vars(p_uid);
      end if;
      return g_lang_id;
  end get_lang;

  function encrypt_un(uid in number, un varchar2, ip varchar2)
        return number
  is
      inputStr varchar2(200);
      hashnum number;
  begin
      --input string
      inputStr := lower(un)||to_char(uid)||to_char(ip);
      hashnum := dbms_utility.get_hash_value(inputStr, 1000, 90000);
      return hashnum;
  end;

    procedure set_pid(p_id number) as
        begin
          g_pid := p_id;
    end;

    function get_pid return number is
    begin
        if (g_pid is null) then
            fill_vars();
        end if;
        return g_pid;
    end get_pid;

    procedure set_java(p_is_java number) is
    begin
        g_is_java := p_is_java;
    end set_java;

    function is_java return number is
        v_ret number;
    begin
        if (g_is_java = 1) then
            v_ret := 1;
        else
            v_ret := 0;
        end if;

        return v_ret;
    end is_java;

    procedure fill_vars(p_uid number default null) is
        v_lang_id app_languages.app_lang_id%type;
        v_pid program.program_id%type;
        v_uid number;
    begin
        if (p_uid is not null) then
            v_uid := p_uid;
        else
            v_uid := get_cu();
        end if;

        if (g_pid is null or g_lang_id is null) then
            v_lang_id := null;
            v_pid := null;
            if v_uid is not null then
               select app_lang_id, program_id into v_lang_id, v_pid from users where user_id = v_uid;
            end if;

            if v_lang_id is null then
                v_lang_id := to_number(pkg_const.get_param_program_val('Default_Language', v_pid));
            end if;

            g_pid := v_pid;
            g_lang_id := v_lang_id;
        end if;
    end fill_vars;

end pkg_sec;
/