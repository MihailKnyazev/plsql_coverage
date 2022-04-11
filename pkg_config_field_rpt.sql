CREATE OR REPLACE PACKAGE BODY PKG_CONFIG_FIELD_RPT 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
is
    pi number default 3.1415926535897932;
    c_value_for_na constant date := to_date('01.01.1900', 'dd.mm.yyyy'); -- Used as a returned constant date value for NA tasks

  function getwfpkey(xitorpk number,
    subXitorPK number,
    p_cfxtid number) return number;


  /**
   * Utility function to simplify access to Drill Down CF values
   * @param p_pk xitor.xitor_id of Primary Trackor Type
   * @param p_xs_cfid config_field_id of Drill Down CF
   * @param p_cfid target CF, actualy used only to get Trackor Type ID, since it is not necessary match obj_xitor_type_id of p_xs_cfid
   * @return xitor.xitor_id for Drill Down field and known xitor_id of Primary Trackor Type
   */
  function getXSkey(
    p_pk number,
    p_xs_cfid config_field.config_field_id%type,
    p_cfid config_field.config_field_id%type)
    return number;


  function getValStr (
    p_key_value in number,
    p_cf_name in config_field.config_field_name%type)
    return varchar2
  as
  begin
    return getValStrByID(p_key_value, pkg_dl_support.get_cf_id(p_cf_name, p_key_value));
  end getValStr;


  function getValStrByID (
      p_key_value in number,
      p_cfid in config_field.config_field_id%type)
      return varchar2
  as
      v_val varchar2(32767);
      v_val_num config_value_number.value_number%type;
      v_vtable_pk number;

      v_cf_row_data_type config_field.data_type%type;
      v_cf_row_is_static config_field.is_static%type;
      v_cf_row_is_show_seconds config_field.is_show_seconds%type;
      v_cf_row_attrib_v_table_id config_field.attrib_v_table_id%type;
      v_cf_row_obj_xitor_type_id config_field.obj_xitor_type_id%type;
      v_cf_row_sql_query config_field.sql_query%type;
      v_lang_id app_languages.app_lang_id%type;
  begin
      select data_type,   is_static,   is_show_seconds,   attrib_v_table_id,   obj_xitor_type_id,   sql_query
        into v_cf_row_data_type, v_cf_row_is_static, v_cf_row_is_show_seconds, v_cf_row_attrib_v_table_id, v_cf_row_obj_xitor_type_id, v_cf_row_sql_query
        from config_field 
       where config_field_id = p_cfid;

    if (v_cf_row_is_static = 1) then
        v_val := getValStrByStaticID(p_key_value, p_cfid);

    elsif (v_cf_row_data_type in (0,30)) then --Text, Hyperlink
        select value_char into v_val 
          from config_value_char
         where config_field_id = p_cfid
           and key_value = p_key_value;

    elsif (v_cf_row_data_type in (1,3)) then --Number,Checkbox
        select to_char(value_number) into v_val
          from config_value_number
         where config_field_id = p_cfid 
           and key_value = p_key_value;

    elsif (v_cf_row_data_type in (11, 12)) then --Latitude, --Longitude
        select case when v_cf_row_data_type = 11 and value_number between -90 and 90 then
                        to_char(value_number, 'FM90D00000000')
                    when v_cf_row_data_type = 12 and value_number between -180 and 180 then
                        to_char(value_number, 'FM990D00000000')
               else to_char(value_number)
               end
          into v_val
          from config_value_number
         where config_field_id = p_cfid 
           and key_value = p_key_value;

    elsif (v_cf_row_data_type in (2)) then --Date
        select to_char(value_date) into v_val
          from config_value_date
         where config_field_id = p_cfid and key_value = p_key_value;

    elsif (v_cf_row_data_type in (90)) then --Date/Time
        if v_cf_row_is_show_seconds = 1 then
            select to_char(value_date, 'MM/DD/YYYY HH24:MI:SS') into v_val
              from config_value_date
             where config_field_id = p_cfid
               and key_value = p_key_value;
        else
            select to_char(value_date, 'MM/DD/YYYY HH24:MI') into v_val
              from config_value_date
             where config_field_id = p_cfid
               and key_value = p_key_value;
        end if;

    elsif (v_cf_row_data_type in (91)) then --Time
        if v_cf_row_is_show_seconds = 1 then
            select to_char(value_date, 'HH24:MI:SS') into v_val
              from config_value_date
             where config_field_id = p_cfid
               and key_value = p_key_value;
        else
            select to_char(value_date, 'HH24:MI') into v_val
              from config_value_date
             where config_field_id = p_cfid
               and key_value = p_key_value;
        end if;

    elsif (v_cf_row_data_type in (4,10)) then --Drop-Down,Selector
        select value_number into v_vtable_pk 
          from config_value_number
         where config_field_id = p_cfid
           and key_value = p_key_value;

        v_val := getVTableLabel(v_vtable_pk, v_cf_row_attrib_v_table_id);

    elsif (v_cf_row_data_type in (5,7)) then --Memo,Rich Memo Editor
        select substr(nvl(value_clob, value_char), 1, 3000) into v_val
          from config_value_char
         where config_field_id = p_cfid
           and key_value = p_key_value;

    elsif (v_cf_row_data_type = 15) then --Electronic File
        select bl.filename into v_val
          from config_value_number cv join blob_data bl on bl.blob_data_id = cv.value_number
         where cv.config_field_id = p_cfid 
           and cv.key_value = p_key_value;

    elsif (v_cf_row_data_type = 16) then --Multiple EFile
        select listagg(b.filename,', ' on overflow truncate without count) 
               within group(order by m.line_number)
          into v_val
          from config_value_mult m
          join blob_data b on b.blob_data_id = m.value_number
         where m.config_field_id = p_cfid
           and m.key_value = p_key_value;

    elsif (v_cf_row_data_type in (20,21)) then --Trackor Selector/Drop-down
        v_val := getTrackorSelectorVal(p_key_value, p_cfid, v_cf_row_obj_xitor_type_id);

    elsif (v_cf_row_data_type in (25,50)) then --Calculated or Rollup
        v_val := getCalcSqlValue(v_cf_row_sql_query, p_key_value);

    elsif (v_cf_row_data_type in (8, 9)) then --DB Drop-Down, EPM DB Selector
        select value_number into v_val_num 
          from config_value_number
         where config_field_id = p_cfid 
           and key_value = p_key_value;

        v_val := getDBSqlValue(v_cf_row_sql_query, v_val_num, p_key_value);

    elsif (v_cf_row_data_type = 22) then --Trackor MultiSelector
        select listagg(pkg_config_field_rpt.get_tr_selector_val_by_xid(m.value_number, m.config_field_id, v_cf_row_obj_xitor_type_id, 0),', '
                       on overflow truncate without count) within group(order by m.line_number)
          into v_val
          from config_value_mult m
         where m.config_field_id = p_cfid
           and m.key_value = p_key_value;

    elsif (v_cf_row_data_type = 80) then --MultiSelector
        v_lang_id := pkg_sec.get_lang;

        select listagg(nvl(vl.label_program_text, tv.value), ', ' on overflow truncate without count)
               within group (order by tv.order_num, nvl(vl.label_program_text, tv.value))
          into v_val
          from config_value_mult m
          join attrib_v_table_value tv on tv.attrib_v_table_value_id = m.value_number
          left join label_program vl on vl.label_program_id = tv.value_label_id 
                                    and vl.app_lang_id = v_lang_id
         where m.config_field_id = p_cfid
           and m.key_value = p_key_value
           and tv.attrib_v_table_id = v_cf_row_attrib_v_table_id;

    else --Blank Spot,Text Label,Horizontal Splitter
        v_val := null;
    end if;

    --I.K.
    --Looks like Excel 2016 works correctly with "=", so I commented out this block
    --
    --Data starting with "=" can be treated incorrectly in Excell reports
    --if substr(v_val, 1, 1) = '=' then
    --   v_val := '''' || v_val;
    --end if;

      return substr(v_val, 1, 3998);
  exception
      when others then
         return null;
  end getValStrByID;



  function getValNumNL (pk number, config_field_text varchar) return number
  as
  begin
    return getValNumNLByID (pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end;


  function getValNumNLByID (pk in xitor.xitor_id%type, config_fieldid in config_field.config_field_id%type) return number
  as
    v_result number;
    v_field_name config_field.config_field_name%type;
    v_static config_field.is_static%type;
    v_key xitor.xitor_key%type; 
    v_class_id xitor.xitor_class_id%type;
  begin
    begin
        select value_number into v_result 
          from config_value_number
         where config_field_id = config_fieldid 
           and key_value = pk;
    exception
        when no_data_found then
            begin
                select x.xitor_key, x.xitor_class_id, cf.is_static, cf.config_field_name
                  into v_key, v_class_id, v_static, v_field_name
                  from config_field cf
                  join xitor x on cf.xitor_type_id = x.xitor_type_id
                 where cf.config_field_id = config_fieldid
                   and x.xitor_id = pk;

                --for numeric Trackor keys only
                if v_field_name = pkg_cf.c_static_xitor_key and v_static = 1 then
                    v_result := to_number(v_key);

                elsif v_field_name = pkg_cf.c_static_xitor_class_id and v_static = 1 then
                    v_result := v_class_id;

                else
                    v_result := null;
                end if;
            exception
                when value_error or no_data_found then
                    v_result := null;
            end;
    end;

    return v_result;
  end getValNumNLByID;


  function getValDate (pk number, config_field_text varchar2) return date
  as
  begin
    return getValDateByID (pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getValDate;


  function getValDateByID (pk number, config_fieldid number) return date
  as
    i date;
  begin
    select value_date into i from config_value_date
    where config_fieldid = config_field_id and key_value = pk;
    return i;
  exception
    when others then
      return null;
  end getValDateByID;

  function getValMemo(p_key_value number, config_field_text varchar2) return clob
  as
  begin
    return getValMemoByID (p_key_value, pkg_dl_support.get_cf_id(config_field_text, p_key_value));
  end getValMemo;

  function getValMemoByID(pk in number, p_cfid in config_field.config_field_id%type) return clob
  as
    v_value_clob clob;
  begin
    select substr(nvl(value_clob, value_char), 1, 30000)
      into v_value_clob
      from config_value_char
     where config_field_id = p_cfid and key_value = pk;

    return v_value_clob;
  exception
    when others then
      return null;
  end getValMemoByID;


  function getLimitedValMemoByID(pk in number, p_cfid in config_field.config_field_id%type) return varchar2
  as
      v_value_clob clob;
      v_value varchar2(4000);
  begin

      begin
          select nvl(value_clob, value_char) into v_value_clob
            from config_value_char
           where config_field_id = p_cfid and key_value = pk;
      exception
          when no_data_found then
              v_value_clob := null;
          when others then raise;
      end;

      v_value := substr(v_value_clob, 1, gv_text_cells_display_limit);
      if length(v_value_clob) > gv_text_cells_display_limit then
         v_value := v_value || pkg_const.gc_truncated_value_suffix;
      end if;

      return v_value;
  exception
      when others then
          return dbms_utility.format_error_backtrace || dbms_utility.format_error_stack;
  end getLimitedValMemoByID;


  function getLimitedValTextByID(pk in number, p_cfid in config_field.config_field_id%type) return varchar2
  as
      v_value config_value_char.value_char%type;
  begin
      begin
          select value_char into v_value
            from config_value_char
           where config_field_id = p_cfid and key_value = pk;
      exception
          when no_data_found then
              v_value := null;
          when others then raise;
      end;

      if length(v_value) > gv_text_cells_display_limit then
         v_value := substr(v_value, 1, gv_text_cells_display_limit) || pkg_const.gc_truncated_value_suffix;
      end if;

      return v_value;
  exception
      when others then
          return dbms_utility.format_error_backtrace || dbms_utility.format_error_stack;
  end getLimitedValTextByID;

  function getLimitedValMemo(pk in number, config_field_text in varchar2) return varchar2
  as
  begin
    return getLimitedValMemoByID(pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getLimitedValMemo;

  function getFullValMemoByID(pk in number, p_cfid in config_field.config_field_id%type) return clob
  as
    v_value_clob clob;
  begin
     select nvl(value_clob, value_char)
      into v_value_clob
      from config_value_char
     where config_field_id = p_cfid and key_value = pk;

    return v_value_clob;
  exception
    when others then
      return null;
  end getFullValMemoByID;

  function getFullValMemo(pk in number, config_field_text in varchar2) return clob
  as
  begin
    return getFullValMemoByID(pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getFullValMemo;

  function getFullValTextByID(
      p_pk in config_value_char.key_value%type, 
      p_cfid in config_value_char.config_field_id%type) 
      return config_value_char.value_char%type
  as
      v_value config_value_char.value_char%type;
  begin
      select value_char
        into v_value
        from config_value_char
       where config_field_id = p_cfid and key_value = p_pk;

      return v_value;
  exception
      when others then
          return null;
  end getFullValTextByID;

  function getLineBlobIDByID (pk number, config_fieldid number, line number) return number
  as
     i number;
  begin
     select cv.value_number into i from config_value_mult cv
     where config_fieldid = cv.config_field_id and key_value = pk and cv.line_number=line;
     return i;
  exception when others then
     return null;
  end getLineBlobIDByID;


  function getLineFileNameByID (pk number, config_fieldid number, line number) return varchar2
  as
    i varchar2(500);
  begin
    select b.filename into i from config_value_mult cv, blob_data b
    where config_fieldid = cv.config_field_id
    and cv.key_value = pk and cv.line_number=line and cv.value_number=b.blob_data_id;
    return i;
  exception when others then
     return null;
  end getLineFileNameByID;


  function getLineFileSizeByID (pk number, config_fieldid number, line number) return number
  as
     i number;
  begin
    select dbms_lob.getlength(b.blob_data) into i from config_value_mult cv, blob_data b
    where config_fieldid = cv.config_FIELD_ID
    and cv.key_value = pk and cv.line_number=line and cv.value_number=b.blob_data_id;
    return i;
  exception when others then
     return 0;
  end getLineFileSizeByID;

  function getValStrYN(pk number, config_field_text varchar2) return varchar2
  as
  begin
    return getValStrByIDYN (pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getValStrYN;


  function getValStrByIDYN(
    p_key_value in number,
    p_cfid in config_field.config_field_id%type)
    return varchar2
  as
    v_val varchar2(16000);
    v_cfdt config_field.data_type%type;
  begin
    select data_type into v_cfdt from config_field where config_field_id = p_cfid;

    v_val := getValStrByID(p_key_value, p_cfid);

    if v_cfdt  = 3 Then --checkbox
      if v_val = '1' Then
        v_val := 'Yes';
      else
        v_val := 'No';
      end if;
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValStrByIDYN;

  function getAllValNumByID (pk number, config_fieldid number) return varchar2
  as
   i varchar2(4000);
  begin
     begin
       i := '';
         for rec in (select cv.value_number
                       from config_field cf, config_value_onlynum cv
                      where cf.config_field_id = config_fieldid
                        and cf.config_field_id = cv.config_field_id
                        and cv.key_value = pk
                        and value_number is not null order by line_number
                    )
       Loop
           i := i || to_char(rec.value_number) || '; ';
       End Loop;
       i := SubStr(i, 0, Length(i)-2);
       exception when others then
         i := null;
       end;
       return i;
  end getAllValNumByID;


  function getAllValNumMultByID (pk number, config_fieldid number) return varchar2
  as
   i varchar2(4000);
  begin
     begin
       i := '';
         For rec in (select cv.value_number
                       from config_field cf, config_value_mult cv
                      where cf.config_field_id = config_fieldid
                        and cf.config_field_id = cv.config_field_id
                        and cv.key_value = pk
                        and value_number is not null order by line_number
                    )
       Loop
           i := i || to_char(rec.value_number) || '; ';
       End Loop;
       i := SubStr(i, 0, Length(i)-2);
       exception when others then
         i := null;
       end;
       return i;
  end getAllValNumMultByID;

    function getAllValNumMultByIDasTabOfNum(
        p_key config_value_mult.key_value%type, 
        p_cfid config_value_mult.config_field_id%type) return tableofnum as
        v_ids tableofnum;
    begin
        select cv.value_number
          bulk collect into v_ids
          from config_value_mult cv
         where cv.config_field_id = p_cfid
           and cv.key_value = p_key
           and value_number is not null 
         order by line_number asc;
         return v_ids;
    end getAllValNumMultByIDasTabOfNum;


  function getAllValNumMultXS(
      p_pk number,
      p_xs_cfid config_field.config_field_id%type,
      p_cfid config_field.config_field_id%type)
      return varchar2
  as
      v_val varchar2(4000);
      v_key_value number;
  begin
    v_key_value := getXSkey(p_pk, p_xs_cfid, p_cfid);

    if (v_key_value is not null) then
       v_val := getAllValNumMultByID(v_key_value, p_cfid);
    end if;

    return v_val;
  end getAllValNumMultXS;


  function getValStrByStaticID (pk number, config_fieldid number) return varchar2
  as
   s varchar2(4000);
   s2 varchar2(4000);
  begin
    for rec in (select f.config_field_name, x.obj_display_field_id
                  from config_field f, xitor_type x
                 where f.config_field_id = config_fieldid
                   and x.xitor_type_id=f.xitor_type_id
               )
    loop
      for recx in (select xitor_key,class_name from xitor where xitor_id = pk)
      loop
        if rec.config_field_name = 'XITOR_KEY' then
           s := recx.xitor_key;
           if rec.obj_display_field_id is not null then
              s2 := getValStrByID(pk, rec.obj_display_field_id);
              if s2 is not null then
                 s := s || ' (' || s2 || ')';
              end if;
           end if;
        elsif rec.config_field_name = 'XITOR_CLASS_ID' then
           s := recx.class_name;
        end if;
      end loop;
    end loop;

     if substr(s,1,1) = '=' then
       s := ''''||s;
     end if;
    return s;
  end getValStrByStaticID;


  function getValStrByStaticName (pk number, config_fieldname varchar2) return varchar2
  as
   s varchar2(4000);
   s2 varchar2(4000);
   fieldname varchar2(100);
   v_obj_display_field_id xitor_type.obj_display_field_id%type;
   v_xitor_key xitor.xitor_key%type;
   v_class_name xitor.class_name%type;
   v_xitor_class_id xitor.xitor_class_id%type;
  begin
    fieldname := config_fieldname;

    if fieldname = 'CLASS_ID' then
       fieldname := 'XITOR_CLASS_ID';
    end if;

    if config_fieldname = 'UN' or config_fieldname = 'EMAIL' then
        begin
            select case when config_fieldname = 'UN' then x.un
                        when config_fieldname = 'EMAIL' then x.email
                   end 
              into s
              from xitor x 
             where x.xitor_id = pk;
        exception
            when no_data_found then
                s := null;
        end;
    elsif config_fieldname = 'IS_DISABLED' then      
        begin
            select case u.is_disabled 
                       when 1 then 'Yes'
                       when 0 then 'No'
                   end case
              into s
              from xitor x join users u on u.xitor_id = x.xitor_id
             where x.xitor_id = pk;
        exception
            when no_data_found then
                s := null;
        end;
    elsif config_fieldname = 'PHONE_NUMBER' then
        begin
            select pkg_vqutils.format_phone_number(p.prefix_code, p.country_code, p.area_code, p.subscriber_code)
              into s
              from xitor x
              join users u on u.xitor_id = x.xitor_id
              join user_phone p on p.user_id = u.user_id
             where x.xitor_id = pk;
        exception
            when no_data_found
                then s := null;
        end;
    elsif fieldname in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id) then
        begin
            select xt.obj_display_field_id,
                   x.xitor_key,
                   x.class_name,
                   x.xitor_class_id
              into v_obj_display_field_id,
                   v_xitor_key,
                   v_class_name,
                   v_xitor_class_id
              from config_field f,
                   xitor_type xt,
                   xitor x
             where f.config_field_name = fieldname
               and x.xitor_id = pk 
               and x.xitor_type_id = xt.xitor_type_id
               and f.is_static = 1
               and xt.xitor_type_id = f.xitor_type_id;

            if config_fieldname = 'XITOR_KEY' then
                s := v_xitor_key;

                if v_obj_display_field_id is not null then
                    s2 := getValStrByID(pk, v_obj_display_field_id);

                    if s2 is not null then
                        s := s || ' (' || s2 || ')';
                    end if;
                end if;
            elsif config_fieldname = 'XITOR_CLASS_ID' then
                s := v_class_name;
            elsif config_fieldname = 'CLASS_ID' then
                s := v_xitor_class_id;
            end if;
        exception
            when no_data_found then
                s := null;
        end;
    end if;

     if substr(s,1,1) = '=' then
       s := ''''||s;
     end if;
    return s;
  end getValStrByStaticName;

  function getDist(AXitor number, ZXitor number, LatField number, LongField number) return number
  as
   s number;
   ALat number;
   ALong number;
   ZLat number;
   ZLong number;
   unitCoef number default 1;
   NumDecimal number default 4;
  begin
    s := Null;
    ALat :=   getValNumNLByID (AXitor, LatField);
    ALong :=  getValNumNLByID (AXitor, LongField);
    ZLat :=   getValNumNLByID (ZXitor, LatField);
    ZLong :=  getValNumNLByID (ZXitor, LongField);

    if Nvl(ALat,999) = 999 or Nvl(ZLat,999) = 999 or Nvl(ALong,999) = 999 or Nvl(ZLong,999) = 999 then
       s := 0;
    else
       ALat := ALat * (Pi / 180);
       ALong := ALong * (Pi / 180);
       ZLat := ZLat * (Pi / 180);
       ZLong := Zlong * (Pi / 180);

       if (ALat = ZLat) and (ALong = ZLong) then
          s := 0;
       else
          s := 6370000 * unitCoef * 2 * (Cos(0.5 * (ALat + ZLat))) * Power(1 - (Cos(ZLat) * Cos(ALat) * Power(Cos((ALong - ZLong) / 2) , 2)) / Power(Cos(0.5 * (ALat + ZLat)) , 2) , (0.5));
          s := Round(s, NumDecimal);
       end if;
    end if;

    return s;
  end getDist;

  function getAzimuth(AXitor number, ZXitor number, LatField number, LongField number, direction varchar default 'AZ') return number
  as
   AZIM number;
   ALat number;
   ALong number;
   ZLat number;
   ZLong number;
   NumDecimal number default 4;
   MA number;
   ALFA  number;
   z number;
  begin
    AZIM := Null;
    ALat :=   getValNumNLByID (AXitor, LatField);
    ALong :=  getValNumNLByID (AXitor, LongField);
    ZLat :=   getValNumNLByID (ZXitor, LatField);
    ZLong :=  getValNumNLByID (ZXitor, LongField);

    if Nvl(ALat,999) = 999 or Nvl(ZLat,999) = 999 or Nvl(ALong,999) = 999 or Nvl(ZLong,999) = 999 then
       AZIM := 0;
    else
       if (ALat = ZLat) and (ALong = ZLong) then
          AZIM := 0;
       else
          ALat := ALat * (Pi / 180);
          ALong := ALong * (Pi / 180);
          ZLat := ZLat * (Pi / 180);
          ZLong := Zlong * (Pi / 180);

          if direction = 'ZA' then
             z := ALat;
             ALat := ZLat;
             ZLat := z;

             z := ALong;
             ALong := ZLong;
             ZLong := z;
          end if;

          MA := ATan(Tan(Pi / 2 - ZLat) * Cos(ZLong - ALong));
          ALFA := ATan(Tan(ZLong - ALong) * Sin(MA) / Cos(ALat + MA)) * 180 / Pi;

          If (ALat > ZLat) And (ALong < ZLong) Then
             AZIM := 180 + ALFA;
          end if;
          If (ALat < ZLat) And (ALong > ZLong) Then
             AZIM := 360 + ALFA;
          end if;
          If (ALat <= ZLat) And (ALong <= ZLong) Then
             AZIM := ALFA;
          end if;
          If (ALat >= ZLat) And (ALong >= ZLong) Then
             AZIM := 180 + ALFA;
          end if;
          If (ALat > ZLat) And (ALong = ZLong) Then
             AZIM := 180;
          end if;
          If (ALat < ZLat) And (ALong = ZLong) Then
             AZIM := 0;
          end if;
          If (ALat = ZLat) And (ALong >= ZLong) Then
             AZIM := 360 + ALFA;
          end if;
          AZIM := Round(AZIM, NumDecimal);
       end if;
    end if;

    return AZIM;
  end getAzimuth;


  function getTimeStampByID (pk number, config_fieldid number) return date
  as
     tmstamp date;
  begin
     select cv.ts into tmstamp
     from config_value cv
     where config_fieldid = cv.config_FIELD_ID
       and key_value = pk;
     return tmstamp;
  exception when others then
     return null;
  end getTimeStampByID;


  function getTimeStamp (pk number, config_field_text varchar2) return date
  as
  begin
    return getTimeStampByID (pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getTimeStamp;


  function getUserIDByID(pk number, config_fieldid number) return number
  as
     userid number;
  begin
     select cv.user_id into userid
     from config_value cv
     where config_fieldid = cv.config_field_id
       and key_value = pk;
     return userid;
  exception when others then
     return null;
  end getUserIDByID;


  function getUserID(pk number, config_field_text varchar2) return number
  as
  begin
    return getUserIDByID(pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getUserID;


  function getUserNameByID(pk number, config_fieldid number) return varchar2
  as
     userid number;
     username varchar2(255);
  begin
     userid := getUserIDByID(pk, config_fieldid);
     if userid is not null then
       select un into username from users where user_id = userid;
     end if;

     return username;
  exception when others then
     return null;
  end getUserNameByID;


  function getUserName(pk number, config_field_text varchar2) return varchar2
  as
  begin
    return getUserNameByID(pk, pkg_dl_support.get_cf_id(config_field_text, pk));
  end getUserName;


  function isFieldInUse(config_fieldid number) return number
  as
      v_ret number := 0;
  begin
      for rec in (select f.config_field_id, f.data_type, f.config_field_name, f.xitor_type_id
                    from config_field f
                   where f.config_field_id = config_fieldid)
      loop
        if rec.data_type in (25, 40, 41, 42) then
            v_ret := 0;
        elsif rec.config_field_name = 'XITOR_KEY' then
            select count(1) into v_ret
              from xitor
             where xitor_type_id = rec.xitor_type_id
               and rownum = 1;
        elsif rec.config_field_name = 'XITOR_CLASS_ID' then
            select count(1) into v_ret
              from xitor
             where xitor_type_id = rec.xitor_type_id
               and xitor_class_id is not null
               and rownum = 1;
        elsif rec.data_type in (16, 22, 80) then
            select count(1) into v_ret
              from config_value_mult v
             where v.config_field_id = rec.config_field_id
               and (v.value_number is not null or v.value_date is not null or v.value_char is not null)
               and rownum = 1;
        else
            if rec.data_type in (1, 3, 4, 8, 9, 10, 11, 12, 15, 20, 21) then
                select count(1) into v_ret
                  from config_value_number v
                 where v.config_field_id = rec.config_field_id
                   and rownum = 1;
            elsif rec.data_type in (2, 90, 91) then
                select count(1) into v_ret
                  from config_value_date v
                 where v.config_field_id = rec.config_field_id
                   and rownum = 1;
            elsif rec.data_type in (0, 30, 5, 7) then
                select count(1) into v_ret
                  from config_value_char v
                 where v.config_field_id = rec.config_field_id
                   and rownum = 1;
            end if;
        end if;
    end loop;

    return v_ret;
  end isFieldInUse;


  function getCalcSqlValue(
    p_calc_sql in config_field.sql_query%type,
    p_key_value in number,
    p_ln in number := null) return varchar2
  as
    v_calc_sql config_field.sql_query%type;
    v_val varchar2(4000);
  begin
    v_calc_sql := regexp_replace(p_calc_sql, ':LN', p_ln, 1, 0, 'i');
    v_calc_sql := regexp_replace(v_calc_sql,'([^[:graph:]|^[:blank:]])',' ');
    v_calc_sql := regexp_replace(v_calc_sql, '--.*['||chr(13)||']', '');
    v_calc_sql := trim(v_calc_sql);
    if substr(v_calc_sql, length(v_calc_sql), 1) <> ';' then
       v_calc_sql := v_calc_sql || ';';
    end if;
    v_calc_sql := 'begin :RETURN_STR := ''''; ' || v_calc_sql || ' end;';

    execute immediate v_calc_sql using out v_val, in p_key_value;
    return v_val;

  exception
    when others then
      return 'Calc field error!';
  end getCalcSqlValue;

  function getDBSqlValue(
    p_sql_query in config_field.sql_query%type,
    p_id in number,
    p_key_value in number,
    p_ln in number := null)

    return varchar2
  AS
    v_source_cursor INTEGER;
    v_col_cnt number;
    v_rec_tab dbms_sql.desc_tab;
    v_sql clob;
    v_rows number;
    v_ret_value varchar2(32767);
    c_err_msg varchar2(5) := 'Error';
  BEGIN
    --Getting column names
    v_source_cursor := dbms_sql.open_cursor;
    begin
      DBMS_SQL.PARSE(v_source_cursor, p_sql_query, DBMS_SQL.NATIVE);
    exception
      when others then
        dbms_sql.close_cursor(v_source_cursor);
        return c_err_msg;
    end;
    dbms_sql.describe_columns(v_source_cursor, v_col_cnt, v_rec_tab);
    dbms_sql.close_cursor(v_source_cursor);

    --Getting DB value by P_KEY_VALUE
    v_sql := empty_clob();
    dbms_lob.createtemporary(v_sql, TRUE);
    dbms_lob.open(v_sql, dbms_lob.lob_readwrite);
    if v_col_cnt >= 2 then
      dbms_lob.append(v_sql, 'select distinct "' || v_rec_tab(2).col_name || '" from (');
    else
      dbms_lob.append(v_sql, 'select distinct "' || v_rec_tab(1).col_name || '" from (');
    end if;
    dbms_lob.copy(v_sql, p_sql_query, dbms_lob.getlength(p_sql_query), dbms_lob.getlength(v_sql) + 1);
    dbms_lob.append(v_sql, ') where ' || v_rec_tab(1).col_name || ' = :p_id');

    v_source_cursor := dbms_sql.open_cursor;
    DBMS_SQL.PARSE(v_source_cursor, v_sql, DBMS_SQL.NATIVE);
    begin
      dbms_sql.bind_variable(v_source_cursor,'PK',p_key_value);
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PARENT_ID', '0');
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PARENT_TRACKOR_TYPE_ID', '0');
    exception
      when others then null;
    end;
    dbms_sql.bind_variable(v_source_cursor,'p_id', p_id);
    dbms_sql.define_column(v_source_cursor, 1, v_ret_value, 32767);
    v_rows := dbms_sql.EXECUTE_AND_FETCH(v_source_cursor);
    IF v_rows > 0 THEN
      DBMS_SQL.COLUMN_VALUE(v_source_cursor, 1, v_ret_value);
    else
      v_ret_value := null;
    END IF;
    dbms_sql.close_cursor(v_source_cursor);
    dbms_lob.close(v_sql);

    if (p_id is not null and v_ret_value is null) then
        v_ret_value := 'Field Value not found!';
    end if;

    RETURN v_ret_value;
  exception
    when others then
    if dbms_sql.is_open(v_source_cursor) then
      dbms_sql.close_cursor(v_source_cursor);
    end if;
    if v_sql is not null and dbms_lob.isopen(v_sql) > 0 then
      dbms_lob.close(v_sql);
    end if;
    return c_err_msg;
  end getDBSqlValue;


  function getDBSqlId(
      p_cfid in config_field.config_field_id%type,
      p_val in varchar2,
      p_program_id in number)
      return number
  as
    p_sql_query config_field.sql_query%type;
    v_source_cursor INTEGER;
    v_col_cnt number;
    v_rec_tab dbms_sql.desc_tab;
    v_sql clob;
    v_rows number;
    v_ret_value varchar2(32767);
    c_err_msg varchar2(5) := 'Error';
    p_tt_id number;
  begin
    for rec in (select sql_query, xitor_type_id
                  from config_field where config_field_id = p_cfid)
    loop
        p_sql_query := rec.sql_query;
        p_tt_id := rec.xitor_type_id;
    end loop;

    if p_sql_query is null then
       return null;
    end if;

    --Getting column names
    v_source_cursor := dbms_sql.open_cursor;
    begin
      DBMS_SQL.PARSE(v_source_cursor, p_sql_query, DBMS_SQL.NATIVE);
    exception
      when others then
        dbms_sql.close_cursor(v_source_cursor);
        return c_err_msg;
    end;
    dbms_sql.describe_columns(v_source_cursor, v_col_cnt, v_rec_tab);
    dbms_sql.close_cursor(v_source_cursor);

    --Getting ID value a given Text value
    v_sql := empty_clob();
    dbms_lob.createtemporary(v_sql, TRUE);
    dbms_lob.open(v_sql, dbms_lob.lob_readwrite);
    if v_col_cnt >= 2 then
      dbms_lob.append(v_sql, 'select distinct ' || v_rec_tab(1).col_name || ' from (');
    else
      return null;
    end if;
    dbms_lob.copy(v_sql, p_sql_query, dbms_lob.getlength(p_sql_query), dbms_lob.getlength(v_sql) + 1);
    dbms_lob.append(v_sql, ') where ');
    dbms_lob.append(v_sql, v_rec_tab(2).col_name);
    dbms_lob.append(v_sql, ' = :p_val');

    v_source_cursor := dbms_sql.open_cursor;
    DBMS_SQL.PARSE(v_source_cursor, v_sql, DBMS_SQL.NATIVE);
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PK', 0);
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PARENT_ID', 0);
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PARENT_TRACKOR_TYPE_ID', 0);
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'PROGRAM_ID', p_program_id);
    exception
      when others then null;
    end;
    begin
      dbms_sql.bind_variable(v_source_cursor, 'TRACKOR_TYPE_ID', p_tt_id);
    exception
      when others then null;
    end;


    dbms_sql.bind_variable(v_source_cursor,'p_val', p_val);
    dbms_sql.define_column(v_source_cursor, 1, v_ret_value, 32767);
    v_rows := dbms_sql.EXECUTE_AND_FETCH(v_source_cursor);
    IF v_rows > 0 THEN
       DBMS_SQL.COLUMN_VALUE(v_source_cursor, 1, v_ret_value);
    else
      v_ret_value := null;
    END IF;
    dbms_sql.close_cursor(v_source_cursor);
    dbms_lob.close(v_sql);

    RETURN to_number(v_ret_value);
  exception
    when others then
    if dbms_sql.is_open(v_source_cursor) then
      dbms_sql.close_cursor(v_source_cursor);
    end if;
    if v_sql is not null and dbms_lob.isopen(v_sql) > 0 then
      dbms_lob.close(v_sql);
    end if;
    return c_err_msg;
  end getDBSqlId;


  function getVTableValueByCfId(
     p_vtable_pk in number,
     p_cfid in config_field.config_field_id%type) return varchar2 as

     v_value varchar2(4000);
     v_table_id config_field.attrib_v_table_id%type;
  begin
     select attrib_v_table_id into v_table_id
     from config_field where config_field_id = p_cfid;

     v_value := getVTableValue(p_vtable_pk,  v_table_id);

     if substr(v_value,1,1) = '=' then
       v_value := '''' || v_value;
     end if;
     return v_value;

  exception
     when no_data_found then
         return null;
  end getVTableValueByCfId;


  function getVTableValue(
     p_vtable_pk in number,
     p_table_id in config_field.attrib_v_table_id%type) return varchar2 as

     v_value varchar2(4000);
  begin
      if (p_table_id is null) or (p_vtable_pk is null) then
          return null;
      end if;

      select value into v_value
      from attrib_v_table_value
      where attrib_v_table_id = p_table_id
      and attrib_v_table_value_id = p_vtable_pk;

      return v_value;
  exception
      when others then
          v_value := p_table_id || ' not found!';
          return v_value;
  end getVTableValue;

  function getVTableLabelByCfId(
     p_vtable_pk in number,
     p_cfid in config_field.config_field_id%type) return label_program.label_program_text%type as

     v_table_id config_field.attrib_v_table_id%type;
  begin
     select attrib_v_table_id into v_table_id
     from config_field where config_field_id = p_cfid;

     return getVTableLabel(p_vtable_pk,  v_table_id);
  exception
     when no_data_found then
         return null;
  end getVTableLabelByCfId;


    function getVTableLabel(
        p_vtable_pk in number,
        p_table_id in config_field.attrib_v_table_id%type) return label_program.label_program_text%type
    as
        v_result label_program.label_program_text%type;
        v_lang_id app_languages.app_lang_id%type;
        v_value_label_id attrib_v_table_value.value_label_id%type;
        v_value attrib_v_table_value.value%type;
    begin
        if (p_table_id is null) or (p_vtable_pk is null) then
            return null;
        end if;

        v_lang_id := pkg_sec.get_lang();

        select value_label_id, value
          into v_value_label_id, v_value
          from attrib_v_table_value
         where attrib_v_table_value_id = p_vtable_pk
           and attrib_v_table_id = p_table_id;

        if v_lang_id is not null and v_value_label_id is not null then
            v_result := coalesce(pkg_label.get_label_program(v_value_label_id, v_lang_id), v_value);
        else
            v_result := v_value;
        end if;

        return v_result;
    exception
        when others then
            v_result := p_table_id || ' not found!';

            return v_result;
    end getVTableLabel;

    function getTrackorSelectorVal(
        p_key_value in number,
        p_cfid in config_field.config_field_id%type,
        p_obj_xtid in config_field.obj_xitor_type_id%type,
        p_ln in number := null,
        p_is_omit_alias in number default 0)
        return varchar2
    as    
        v_xitor_id xitor.xitor_id%type;        
    begin
        if p_ln is null then
            select value_number into v_xitor_id
              from config_value_number
             where config_field_id = p_cfid
               and key_value = p_key_value;
        else
            select value_number into v_xitor_id
              from config_value_mult
             where config_field_id = p_cfid
               and key_value = p_key_value
               and line_number = p_ln;
        end if;

        return get_tr_selector_val_by_xid(v_xitor_id, p_cfid, p_obj_xtid, p_is_omit_alias);
    exception
        when no_data_found then
            return null;
    end getTrackorSelectorVal;


    function get_tr_selector_val_by_xid(
        p_xid in xitor.xitor_id%type,
        p_cfid in config_field.config_field_id%type,
        p_obj_xtid in config_field.obj_xitor_type_id%type,
        p_is_omit_alias in number default 0)
        return varchar2
    as
        v_val varchar2(4000);
        v_xkey xitor.xitor_key%type;
        v_alias_cfid xitor_type.obj_display_field_id%type;
        v_obj_cfid config_field.obj_config_field_id%type;
    begin

        if p_xid is not null then
            begin
                select xitor_key into v_val from xitor
                where xitor_id = p_xid;
            exception
            when others then
                return pkg_label.get_label_system_or_def(6514, pkg_sec.get_lang(), pkg_sec.get_pid());
            end;
        end if;

        select obj_config_field_id into v_obj_cfid
        from config_field where config_field_id = p_cfid;

        if (v_obj_cfid is not null) then
            v_val := getValStrByID(p_xid, v_obj_cfid);
        elsif (p_is_omit_alias <> 1) then
            select obj_display_field_id into v_alias_cfid
            from  xitor_type where xitor_type_id = p_obj_xtid;

            if (v_alias_cfid is not null) and (p_xid is not null) then
                v_xkey := pkg_config_field_rpt.getValStrByID(p_xid, v_alias_cfid);

                if v_xkey is not null then
                    v_val := v_val || ' (' || v_xkey || ')';
                end if;
            end if;
        end if;

        return v_val;
    end get_tr_selector_val_by_xid;

    function get_trackor_key(
        p_xid in xitor.xitor_id%type,
        p_is_omit_alias in number default 0)
        return varchar2 as

        v_val varchar2(4000);
        v_alias_val xitor.xitor_key%type;
        v_alias_cfid xitor_type.obj_display_field_id%type;
    begin
        begin
            select x.xitor_key, 
                   xt.obj_display_field_id
              into v_val,
                   v_alias_cfid
              from xitor x
              join xitor_type xt on (xt.xitor_type_id = x.xitor_type_id)
             where xitor_id = p_xid;
        exception
            when no_data_found then
                v_val := null;
                v_alias_cfid := null;
        end;

        if p_is_omit_alias <> 1 and v_val is not null and v_alias_cfid is not null then
            v_alias_val := pkg_config_field_rpt.getValStrByID(p_xid, v_alias_cfid);
            if v_alias_val is not null then
                v_val := v_val || ' (' || v_alias_val || ')';
            end if;
        end if;

        if v_val is null then
            v_val := pkg_label.get_label_system_or_def(6514, pkg_sec.get_lang(), pkg_sec.get_pid());
        end if;

        return substrb(v_val, 1, 4000);
    end get_trackor_key;


  function getValStrStaticXS(pk number, xs_field_id  number, cf_xt_id  number, config_fieldname varchar2) return varchar2
  as
    v_val varchar2(4000);
    p_key_value number;
    v_obj_xitor_type_id config_field.obj_xitor_type_id%type;
  begin
    v_val := null;
    select obj_xitor_type_id into v_obj_xitor_type_id from config_field where config_field_id = xs_field_id;

    if v_obj_xitor_type_id = cf_xt_id then
       --The field is from XS level
       for rec in (select v.value_number from config_value_number v
              where v.config_field_id=xs_field_id and v.key_value = pk)
       loop
           p_key_value := rec.value_number;
       end loop;
    else
       --The field belongs to parent of XS
       for rec in (select a.parent_id
                     from ancestor a, config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pk
                      and a.child_id=v.value_number     and a.p_xitor_type_id=cf_xt_id
                      and a.c_xitor_type_id=v_obj_xitor_type_id)
       loop
           p_key_value := rec.parent_id;
       end loop;
    end if;

    if p_key_value is not null then
       if config_fieldname = 'XITOR_KEY' then
          v_val := getValStrByStaticName(p_key_value, config_fieldname);
       elsif config_fieldname = 'IS_DISABLED' then
          v_val := '';
          for rec in (select IS_DISABLED from users where xitor_id = p_key_value)
          loop
             v_val := to_char(rec.is_disabled);
          end loop;
          if v_val = '1' Then
             v_val := 'Yes';
          elsif v_val = '0' Then
             v_val := 'No';
          end if;
       else
          for rec in (select XITOR_CLASS_ID, CLASS_NAME, UN, EMAIL from xitor where xitor_id = p_key_value)
          loop
             if config_fieldname = 'XITOR_CLASS_ID' then
                v_val := rec.class_name;
             elsif config_fieldname = 'CLASS_ID' then
                v_val := rec.xitor_class_id;
             elsif config_fieldname = 'UN' then
                v_val := rec.un;
             elsif config_fieldname = 'EMAIL' then
                v_val := rec.email;
             end if;
          end loop;
       end if;
    end if;

    --Data starting with "=" can be treated incorrectly in Excell reports
    if substr(v_val, 1, 1) = '=' then
      v_val := '''' || v_val;
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValStrStaticXS;


  function getValStrStaticXSID(pk number, xs_field_id  number, cf_xt_id  number) return varchar2
  as
    p_key_value number;
    v_obj_xitor_type_id config_field.obj_xitor_type_id%type;
  begin
    select obj_xitor_type_id into v_obj_xitor_type_id from config_field where config_field_id = xs_field_id;

    if v_obj_xitor_type_id = cf_xt_id then
       --The field is from XS level
       for rec in (select v.value_number from config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pk)
       loop
           p_key_value := rec.value_number;
       end loop;
    else
       --The field belongs to parent of XS
       for rec in (select a.parent_id from ancestor a, config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pk
                      and a.child_id=v.value_number and a.p_xitor_type_id=cf_xt_id and a.c_xitor_type_id=v_obj_xitor_type_id)
       loop
           p_key_value := rec.parent_id;
       end loop;
    end if;

    return p_key_value;
  exception
    when others then
      return null;
  end getValStrStaticXSID;


  function getXSkey(
    p_pk number,
    p_xs_cfid config_field.config_field_id%type,
    p_cfid config_field.config_field_id%type)
    return number
  as
    v_cf_ttid config_field.xitor_type_id%type;
    v_obj_ttid config_field.obj_xitor_type_id%type;
    v_key_value number;
  begin
    select obj_xitor_type_id into v_obj_ttid from config_field where config_field_id = p_xs_cfid;
    select xitor_type_id into v_cf_ttid from config_field where config_field_id = p_cfid;

    begin
      if v_obj_ttid = v_cf_ttid then
         select v.value_number into v_key_value from config_value_number v
         where v.config_field_id=p_xs_cfid and v.key_value = p_pk;

      else
         select a.parent_id into v_key_value
         from ancestor a, config_value_number v
         where v.config_field_id=p_xs_cfid and v.key_value = p_pk
         and a.child_id=v.value_number and a.p_xitor_type_id=v_cf_ttid
         and a.c_xitor_type_id=v_obj_ttid;
      end if;
    exception
      when no_data_found then
        v_key_value := null;
      when too_many_rows then
        --normaly user can't select fields from parent many-many relation to the gird,
        --maybe relation type were changed after user selected rows?
        raise;
    end;

    return v_key_value;
  end getXSkey;


  function getValStrXS(
    p_pk number,
    p_xs_cfid config_field.config_field_id%type,
    p_cfid config_field.config_field_id%type)
    return varchar2
  as
    v_val varchar2(4000) := null;
    v_key_value number;
  begin
    v_key_value := getXSkey(p_pk, p_xs_cfid, p_cfid);
    if (v_key_value is not null) then
       v_val := getValStrByIDYN(v_key_value, p_cfid);
    end if;
    return v_val;
  end getValStrXS;

  function getLimitedValMemoXSID(pk number, xs_field_id  number, config_fieldid  number) return varchar2
    as
    v_value varchar2(4000) := null;
    v_key_value number;
  begin
    v_key_value := getXSkey(pk, xs_field_id, config_fieldid);
    if (v_key_value is not null) then
       v_value := getLimitedValMemoByID(v_key_value, config_fieldid);
    end if;
    return v_value;
  end;

  function getValNumNLXS(
    p_pk number,
    p_xs_cfid config_field.config_field_id%type,
    p_cfid config_field.config_field_id%type) return number
  as
    v_val number;
    v_key_value number;
  begin
    v_key_value := getXSkey(p_pk, p_xs_cfid, p_cfid);
    if (v_key_value is not null) then
       v_val := getValNumNLByID(v_key_value, p_cfid);
    end if;
    return v_val;
  end getValNumNLXS;


  function getValDateXS(
    p_pk number,
    p_xs_cfid config_field.config_field_id%type,
    p_cfid config_field.config_field_id%type)
    return date
  as
    v_val date;
    v_key_value number;
  begin
    v_key_value := getXSkey(p_pk, p_xs_cfid, p_cfid);
    if (v_key_value is not null) then
       v_val := getValDateByID(v_key_value, p_cfid);
    end if;
    return v_val;
  end getValDateXS;


  function getValStrXSID(pk number, xs_field_id  number, config_fieldid  number) return varchar2
  as    
    p_key_value number;
    cf_xt_id number;
    v_obj_xitor_type_id config_field.obj_xitor_type_id%type;
  begin
    select obj_xitor_type_id into v_obj_xitor_type_id from config_field where config_field_id = xs_field_id;
    select xitor_type_id into cf_xt_id from config_field where config_field_id = config_fieldid;

    if v_obj_xitor_type_id = cf_xt_id then
       --The field is from XS level
       for rec in (SELECT v.value_number FROM config_value_number v
              WHERE v.config_field_id=xs_field_id and v.key_value = pk)
       loop
           p_key_value := rec.value_number;
       end loop;
    else
       --The field belongs to parent of XS
       for rec in (select a.parent_id
                     from ancestor a, config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pk
                      and a.child_id=v.value_number     and a.p_xitor_type_id=cf_xt_id and a.c_xitor_type_id=v_obj_xitor_type_id)
       loop
           p_key_value := rec.parent_id;
       end loop;
    end if;

    if p_key_value is not null then
       p_key_value := getValNumNLByID(p_key_value, config_fieldid);
    end if;

    return p_key_value;
  exception
    when others then
      return null;
  end getValStrXSID;


  function getValStrStaticParent(pk number, c_xt_id  number,  p_xt_id  number, config_fieldname varchar2) return varchar2
  as
    v_val varchar2(4000);
    p_key_value number;
  begin
    v_val := null;

    for rec in (SELECT a.parent_id FROM ancestor a
        WHERE a.child_id=pk and a.p_xitor_type_id=p_xt_id and a.c_xitor_type_id=c_xt_id)
    loop
        p_key_value := rec.parent_id;
    end loop;


    if p_key_value is not null then
       if config_fieldname = 'XITOR_KEY' then
          v_val := getValStrByStaticName(p_key_value, config_fieldname);
       elsif config_fieldname = 'IS_DISABLED' then
          v_val := '';
          for rec in (select IS_DISABLED from users where xitor_id = p_key_value)
          loop
             v_val := to_char(rec.is_disabled);
          end loop;
          if v_val = '1' Then
             v_val := 'Yes';
          elsif v_val = '0' Then
             v_val := 'No';
          end if;
       else
          for rec in (select XITOR_CLASS_ID, CLASS_NAME, UN, EMAIL from xitor where xitor_id = p_key_value)
          loop
             if config_fieldname = 'XITOR_CLASS_ID' then
                v_val := rec.class_name;
             elsif config_fieldname = 'CLASS_ID' then
                v_val := rec.xitor_class_id;
             elsif config_fieldname = 'UN' then
                v_val := rec.un;
             elsif config_fieldname = 'EMAIL' then
                v_val := rec.email;
             end if;
          end loop;
       end if;
    end if;

    --Data starting with "=" can be treated incorrectly in Excell reports
    if substr(v_val, 1, 1) = '=' then
      v_val := '''' || v_val;
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValStrStaticParent;


 function getValStrStaticParentID(pk number, c_xt_id  number,  p_xt_id  number) return varchar2
  as
    p_key_value number;
  begin
    for rec in (SELECT a.parent_id FROM ancestor a
        WHERE a.child_id=pk and a.p_xitor_type_id=p_xt_id and a.c_xitor_type_id=c_xt_id)
    loop
        p_key_value := rec.parent_id;
    end loop;

    return p_key_value;
  exception
    when others then
      return null;
  end getValStrStaticParentID;

  function getValStrByParentID(pk in number, cxitor_type_id number,  pxitor_type_id number, config_fieldid in config_field.config_field_id%type)
  return varchar2
  as
    v_val varchar2(4000);
    p_key_value number;
  begin
    v_val := null;
    --get ParentID
    for rec in (select a.parent_id from ancestor a
                 where a.child_id=pk and a.p_xitor_type_id=pxitor_type_id and a.c_xitor_type_id=cxitor_type_id)
    loop
        p_key_value := rec.parent_id;
    end loop;

    if p_key_value is not null then
       v_val := getValStrByIDYN(p_key_value, config_fieldid);
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValStrByParentID;

  function getValNumNLByParentID(pk in number, cxitor_type_id number,  pxitor_type_id number, config_fieldid in config_field.config_field_id%type)
  return number
  as
    v_val number;
    p_key_value number;
  begin
    v_val := null;
    --get ParentID
    for rec in (select a.parent_id from ancestor a
                 where a.child_id=pk and a.p_xitor_type_id=pxitor_type_id and a.c_xitor_type_id=cxitor_type_id)
    loop
        p_key_value := rec.parent_id;
    end loop;

    if p_key_value is not null then
       v_val := getValNumNLByID(p_key_value, config_fieldid);
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValNumNLByParentID;


  function getValXSIDByParentID(pk in number, cxitor_type_id number,  pxitor_type_id number, config_fieldid in config_field.config_field_id%type)
  return varchar2
  as
    v_val varchar2(4000);
    p_key_value number;
  begin
    v_val := null;
    --get ParentID
    for rec in (select a.parent_id from ancestor a
                 where a.child_id=pk and a.p_xitor_type_id=pxitor_type_id and a.c_xitor_type_id=cxitor_type_id)
    loop
        p_key_value := rec.parent_id;
    end loop;

    if p_key_value is not null then
       for rec in (select v.value_number from config_value_number v
                    where v.config_field_id=config_fieldid and v.key_value = p_key_value)
       loop
           v_val := rec.value_number;
       end loop;
    end if;

    return v_val;
  exception
    when others then
      return null;
  end getValXSIDByParentID;


  function getTaskDate(wpid number, xitorID number, subXitorID number, TemplateTaskID number, dateType number, SF number) return date
  as
   d date;
  begin
    d := null;

    if wpid is null or xitorID is null then
       return d;
    end if;

    if dateType <= 3 then
       for rec in (select start_baseline_date, finish_baseline_date, start_projected_date, finish_projected_date, start_promised_date, finish_promised_date, start_actual_date, finish_actual_date
                     from wp_tasks t
                    where t.wp_workplan_id = wpid and t.template_task_id = TemplateTaskID
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null)
                      and t.is_not_applicable = 0)
       loop
           if dateType = 0 and SF = 0 then
              d := rec.start_baseline_date;
           elsif dateType = 0 and SF = 1 then
              d := rec.finish_baseline_date;
           elsif dateType = 1 and SF = 0 then
              d := rec.start_projected_date;
           elsif dateType = 1 and SF = 1 then
              d := rec.finish_projected_date;
           elsif dateType = 2 and SF = 0 then
              d := rec.start_promised_date;
           elsif dateType = 2 and SF = 1 then
              d := rec.finish_promised_date;
           elsif dateType = 3 and SF = 0 then
              d := rec.start_actual_date;
           elsif dateType = 3 and SF = 1 then
              d := rec.finish_actual_date;
           end if;
       end loop;
    else
       for rec in (select dt.start_date , dt.finish_date
                     from wp_tasks t, wp_task_dates dt
                    where t.wp_workplan_id = wpid and t.template_task_id = TemplateTaskID
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID) and t.is_not_applicable = 0
                      and dt.wp_task_id=t.wp_task_id and dt.wp_task_date_type_id = dateType
                   )
       loop
           if SF = 0 then
              d := rec.start_date;
           elsif SF = 1 then
              d := rec.finish_date;
           end if;
       end loop;
    end if;
    return d;
  end getTaskDate;
  function getTaskDateSummary(wpid number, xitorID number, subXitorID number, TemplateTaskID number) return date
  as
   d date;
  begin
    d := null;
    if wpid is null or xitorID is null then
      return d;
    end if;

    for rec in (select t.finish_actual_date, t.finish_projected_date
                  from wp_tasks t
                 where t.wp_workplan_id = wpid and t.template_task_id = TemplateTaskID
                   and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0)
    loop
        if rec.finish_actual_date is not null then
           d := rec.finish_actual_date;
        elsif rec.finish_projected_date is not null then
           d := rec.finish_projected_date;
        end if;
    end loop;

    return d;
  end getTaskDateSummary;

  function getCFTaskDateByTaskID(taskid number, dateType number, SF number) return date
  as
   d date;
  begin
    d := null;

    for rec in (select dt.start_date, dt.finish_date
                  from wp_task_dates dt
                 where dt.wp_task_id = taskid and dt.wp_task_date_type_id = dateType)
    loop
       if SF = 0 then
          d := rec.start_date;
       elsif SF = 1 then
          d := rec.finish_date;
       end if;
    end loop;

    return d;
  end getCFTaskDateByTaskID;

  function getTNameDate(wpid number, xitorID number, subXitorID number, taskNameId number, dateType number, SF number) return date
  as
   d date;
  begin
    d := null;
    if wpid is null or xitorID is null or taskNameId is null or dateType is null then
       return d;
    end if;

    if dateType <= 3 then
       for rec in (select t.start_baseline_date, t.finish_baseline_date, t.start_projected_date, t.finish_projected_date,
                          t.start_promised_date, t.finish_promised_date, t.start_actual_date, t.finish_actual_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_name_ids wtids
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id = wp.wp_workplan_id
                      and wtids.task_name_id = taskNameId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
                  )
       loop
           if dateType = 0 and SF = 0 then
              d := rec.start_baseline_date;
           elsif dateType = 0 and SF = 1 then
              d := rec.finish_baseline_date;
           elsif dateType = 1 and SF = 0 then
              d := rec.start_projected_date;
           elsif dateType = 1 and SF = 1 then
              d := rec.finish_projected_date;
           elsif dateType = 2 and SF = 0 then
              d := rec.start_promised_date;
           elsif dateType = 2 and SF = 1 then
              d := rec.finish_promised_date;
           elsif dateType = 3 and SF = 0 then
              d := rec.start_actual_date;
           elsif dateType = 3 and SF = 1 then
              d := rec.finish_actual_date;
           end if;
       end loop;
    else
       for rec in (select dt.start_date, dt.finish_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_name_ids wtids, wp_task_dates dt
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id = wp.wp_workplan_id
                      and wtids.task_name_id = taskNameId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
                      and dt.wp_task_id=t.wp_task_id and dt.wp_task_date_type_id = dateType
                  )
       loop
           if SF = 0 then
              d := rec.start_date;
           elsif SF = 1 then
              d := rec.finish_date;
           end if;
       end loop;
    end if;
    return d;
  end getTNameDate;

  function getTOrdDate(wpid number, xitorID number, subXitorID number, ordnumId number, dateType number, SF number) return date
  as
   d date;
  begin
    d := null;
    if wpid is null or xitorID is null or ordnumId is null or dateType is null then
       return d;
    end if;

    if dateType <= 3 then
       for rec in (select t.start_baseline_date, t.finish_baseline_date, t.start_projected_date, t.finish_projected_date,
                          t.start_promised_date, t.finish_promised_date, t.start_actual_date, t.finish_actual_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_ordnum_ids wtids
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id  = wp.wp_workplan_id
                      and wtids.ordnum_id   = ordnumId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0)
       loop
           if dateType = 0 and SF = 0 then
              d := rec.start_baseline_date;
           elsif dateType = 0 and SF = 1 then
              d := rec.finish_baseline_date;
           elsif dateType = 1 and SF = 0 then
              d := rec.start_projected_date;
           elsif dateType = 1 and SF = 1 then
              d := rec.finish_projected_date;
           elsif dateType = 2 and SF = 0 then
              d := rec.start_promised_date;
           elsif dateType = 2 and SF = 1 then
              d := rec.finish_promised_date;
           elsif dateType = 3 and SF = 0 then
              d := rec.start_actual_date;
           elsif dateType = 3 and SF = 1 then
              d := rec.finish_actual_date;
           end if;
       end loop;
    else
       for rec in (select dt.start_date, dt.finish_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_ordnum_ids wtids, wp_task_dates dt
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id = wp.wp_workplan_id
                      and wtids.ordnum_id = ordnumId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
                      and dt.wp_task_id=t.wp_task_id and dt.wp_task_date_type_id = dateType
                  )
       loop
           if SF = 0 then
              d := rec.start_date;
           elsif SF = 1 then
              d := rec.finish_date;
           end if;
       end loop;
    end if;
    return d;
  end getTOrdDate;

  function getTWbsDate(wpid number, xitorID number, subXitorID number, wbsId number, dateType number, SF number) return date
  as
   d date;
  begin
    d := null;
    if wpid is null or xitorID is null or wbsId is null or dateType is null then
       return d;
    end if;

    if dateType <= 3 then
       for rec in (select t.start_baseline_date, t.finish_baseline_date, t.start_projected_date, t.finish_projected_date,
                          t.start_promised_date, t.finish_promised_date, t.start_actual_date, t.finish_actual_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_wbs_ids wtids
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id = wp.wp_workplan_id
                      and wtids.wbs_id = wbsId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
                  )
       loop
           if dateType = 0 and SF = 0 then
              d := rec.start_baseline_date;
           elsif dateType = 0 and SF = 1 then
              d := rec.finish_baseline_date;
           elsif dateType = 1 and SF = 0 then
              d := rec.start_projected_date;
           elsif dateType = 1 and SF = 1 then
              d := rec.finish_projected_date;
           elsif dateType = 2 and SF = 0 then
              d := rec.start_promised_date;
           elsif dateType = 2 and SF = 1 then
              d := rec.finish_promised_date;
           elsif dateType = 3 and SF = 0 then
              d := rec.start_actual_date;
           elsif dateType = 3 and SF = 1 then
              d := rec.finish_actual_date;
           end if;
       end loop;
    else
       for rec in (select dt.start_date, dt.finish_date
                     from wp_tasks t, wp_workplan wp, wp_tasks_wbs_ids wtids, wp_task_dates dt
                    where wp.wp_workplan_id = wpid and wtids.template_workplan_id = wp.template_workplan_id
                      and t.wp_workplan_id = wp.wp_workplan_id
                      and wtids.wbs_id = wbsId and t.template_task_id = wtids.template_task_id
                      and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
                      and dt.wp_task_id=t.wp_task_id and dt.wp_task_date_type_id = dateType
                  )
       loop
           if SF = 0 then
              d := rec.start_date;
           elsif SF = 1 then
              d := rec.finish_date;
           end if;
       end loop;
    end if;
    return d;
  end getTWbsDate;

  function getFluxxCF (pkey number, field varchar2) return date
  as
     tmstamp date;
     fld varchar2(100);
  begin
     if field='XITOR_KEY' then
        fld := '0';
     elsif field='XITOR_CLASS_ID' then
        fld := '4';
     else
        fld := field;
     end if;

     select max(t.ts) into tmstamp
      from audit_log_cflite t
      where t.column_name=fld and t.pk=pkey;

     return tmstamp;
  exception when others then
     return null;
  end getFluxxCF;

  function getFluxxCFParent(pkey number, c_xt_id  number,  p_xt_id  number, field varchar2) return date
  as
    p_key_value number;
    tmstamp date;
  begin
    tmstamp := null;

    for rec in (select a.parent_id from ancestor a
                 where a.child_id=pkey and a.p_xitor_type_id=p_xt_id and a.c_xitor_type_id=c_xt_id)
    loop
        p_key_value := rec.parent_id;
    end loop;

    if p_key_value is not null then
       tmstamp := getFluxxCF(p_key_value, field);
    end if;

    return tmstamp;
  exception
    when others then
      return null;
  end getFluxxCFParent;

  function getFluxxXS(pkey number, xs_field_id  number, field number) return date
  as
    tmstamp date;
    cf_xt_id number;
    cf_data_type number;
    p_key_value number;
    v_obj_xitor_type_id config_field.obj_xitor_type_id%type;
  begin
    tmstamp := null;
    select obj_xitor_type_id into v_obj_xitor_type_id from config_field where config_field_id = xs_field_id;
    select xitor_type_id, data_type into cf_xt_id, cf_data_type from config_field where config_field_id = field;

    if v_obj_xitor_type_id = cf_xt_id then
       --The field is from XS level
       for rec in (select v.value_number from config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pkey)
       loop
           p_key_value := rec.value_number;
       end loop;
    else
       --The field belongs to parent of XS
       for rec in (select a.parent_id from ancestor a, config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pkey
                      and a.child_id=v.value_number and a.p_xitor_type_id=cf_xt_id and a.c_xitor_type_id=v_obj_xitor_type_id)
       loop
           p_key_value := rec.parent_id;
       end loop;
    end if;

    if p_key_value is not null then
       tmstamp := getFluxxCF(p_key_value, field);
    end if;

    return tmstamp;
  exception
    when others then
      return null;
  end getFluxxXS;

function getFluxxStaticXS(pkey number, xs_field_id  number,  cf_xt_id  number, field varchar2) return date
  as
    tmstamp date;
    p_key_value number;
    v_obj_xitor_type_id config_field.obj_xitor_type_id%type;
  begin
    tmstamp := null;
    select obj_xitor_type_id into v_obj_xitor_type_id from config_field where config_field_id = xs_field_id;

    if v_obj_xitor_type_id = cf_xt_id then
       --The field is from XS level
       for rec in (select v.value_number from config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pkey)
       loop
           p_key_value := rec.value_number;
       end loop;
    else
       --The field belongs to parent of XS
       for rec in (select a.parent_id
                     from ancestor a, config_value_number v
                    where v.config_field_id=xs_field_id and v.key_value = pkey
                      and a.child_id=v.value_number and a.p_xitor_type_id=cf_xt_id and a.c_xitor_type_id=v_obj_xitor_type_id
                  )
       loop
           p_key_value := rec.parent_id;
       end loop;
    end if;

    if p_key_value is not null then
       tmstamp := getFluxxCF(p_key_value, field);
    end if;

    return tmstamp;
  exception
    when others then
      return null;
  end getFluxxStaticXS;

  function getFluxxTask (wpid number, xitorID number, subXitorID number, TemplateTaskID number, dateType number, SF number) return date
  as
     tmstamp date;
     fld varchar2(100);
     p_key_value number;
  begin
    tmstamp := null;
    if wpid is null or xitorID is null then
       return tmstamp;
    end if;

    for rec in (select wp_task_id from wp_tasks t
                 where t.wp_workplan_id = wpid and t.template_task_id = TemplateTaskID
                   and (t.xitor_id = xitorID or t.xitor_id = subXitorID or t.xitor_id is null) and t.is_not_applicable = 0
               )
    loop
        p_key_value := rec.wp_task_id;
    end loop;

    if p_key_value is not null then
       if dateType <= 3 then
          if dateType = 0 then
             fld := 'BASELINE_DATE';
          elsif dateType = 1 then
             fld := 'PROJECTED_DATE';
          elsif dateType = 2 then
             fld := 'PROMISED_DATE';
          elsif dateType = 3 then
             fld := 'ACTUAL_DATE';
          end if;

          if SF = 0 then
             fld := 'START_' || fld;
          else
             fld := 'FINISH_' || fld;
          end if;
       else
          fld := to_char(dateType) || '_' || to_char(SF);
       end if;

       select max(t.ts) into tmstamp
        from audit_log_tlite t
        where t.column_name=fld and t.pk=p_key_value;
    end if;

    return tmstamp;
  exception when others then
     return null;
  end getFluxxTask;


  function getWFValStrByIDYN(
    xitorPK number,
    subXitorPK number,
    p_cfxtid number,
    p_cfid number)
    return varchar2
  as
    v_val varchar2(4000);
    pKey number;
  begin
    if xitorPK is Null and subXitorPK is Null then
       v_val := null;
    else
      pKey := getWFPKey(xitorPK, subXitorPK, p_cfxtid);
      if pKey is not Null then
         if p_cfid = 0 then
            v_val := getValStrByStaticName(pKey, 'XITOR_KEY');
         elsif p_cfid = 4 then
            v_val := getValStrByStaticName(pKey, 'XITOR_CLASS_ID');
         else
            v_val := getValStrByIDYN(pKey, p_cfid);
         end if;
      end if;
    end if;
    return v_val;
  exception
    when others then
      return null;
  end getWFValStrByIDYN;


  function getWFValNumNLByID(
    xitorPK number,
    subXitorPK number,
    p_cfxtid number,
    p_cfid number)
    return varchar2
  as
    v_val varchar2(4000);
    pKey number;
  begin
    if xitorPK is Null and subXitorPK is Null then
       v_val := null;
    else
      pKey := getWFPKey(xitorPK, subXitorPK, p_cfxtid);
      if pKey is not Null then
         v_val := getValNumNLByID(pKey, p_cfid);
      end if;
    end if;
    return v_val;
  exception
    when others then
      return null;
  end getWFValNumNLByID;


  function getWFValDateByID(
    xitorPK number,
    subXitorPK number,
    p_cfxtid number,
    p_cfid number)
    return varchar2
  as
    v_val varchar2(4000);
    pKey number;
  begin
    if xitorPK is Null and subXitorPK is Null then
       v_val := null;
    else
      pKey := getWFPKey(xitorPK, subXitorPK, p_cfxtid);
      if pKey is not Null then
         v_val := getValDateByID(pKey, p_cfid);
      end if;
    end if;
    return v_val;
  exception
    when others then
      return null;
  end getWFValDateByID;


  function getWFPKey(xitorPK number,
    subXitorPK number,
    p_cfxtid number) return number
  as
    xitorXTID number;
    subXitorXTID number;
    pKey number;
  begin
    if xitorPK is Null and subXitorPK is Null then
       pKey := null;
    else
      xitorXTID := -1;
      subXitorXTID := -1;
      if xitorPK is not Null then
         select xitor_type_id into xitorXTID from xitor where xitor_id=xitorPK;
      end if;
      if subXitorPK is not Null then
         select xitor_type_id into subXitorXTID from xitor where xitor_id=subXitorPK;
      end if;

      if p_cfxtid = xitorXTID then
         pKey := xitorPK;
         --The field is from Xitor level
      elsif p_cfxtid = subXitorXTID then
         --The field is from Sub-Xitor level
         pKey := subXitorPK;
      else
         --The field is from Parent level
         for rec in (select a.parent_id from ancestor a
                      where a.child_id=xitorpk and a.p_xitor_type_id=p_cfxtid and a.c_xitor_type_id=xitorXTID)
         loop
             pKey := rec.parent_id;
         end loop;
      end if;
    end if;
    return pKey;
  exception
    when others then
      return null;
  end getWFPKey;


    function get_trackor_ids_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type) return tableofnum
    as
        v_ret tableofnum;
    begin
        select child_id
          bulk collect into v_ret
          from ancestor
         where parent_id = p_current_tid
           and p_xitor_type_id = p_current_tt_id
           and c_xitor_type_id = p_child_tt_id;

        return v_ret;
    end get_trackor_ids_collection_child;

    function get_trackor_ids_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type) return tableofnum
    as
        v_ret tableofnum;
    begin
        select parent_id
          bulk collect into v_ret
          from ancestor
         where child_id = p_current_tid
           and c_xitor_type_id = p_current_tt_id
           and p_xitor_type_id = p_parent_tt_id;

        return v_ret;
    end get_trackor_ids_collection_parent;

    function get_val_num_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofnum
    as
        v_ret tableofnum;
    begin
        select value_number
          bulk collect into v_ret
          from config_value_number
         where key_value in (select child_id from ancestor
                              where parent_id = p_current_tid
                                and p_xitor_type_id = p_current_tt_id
                                and c_xitor_type_id = p_child_tt_id)
           and config_field_id = p_child_cf_id;

        return v_ret;
    end get_val_num_collection_child;

    function get_val_char_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofchar
    as
        v_ret tableofchar;
    begin
        select value_char
          bulk collect into v_ret
          from config_value_char
         where key_value in (select child_id from ancestor
                              where parent_id = p_current_tid
                                and p_xitor_type_id = p_current_tt_id
                                and c_xitor_type_id = p_child_tt_id)
           and config_field_id = p_child_cf_id;

        return v_ret;
    end get_val_char_collection_child;

    function get_val_clob_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofclob
    as
        v_ret tableofclob;
    begin
        select nvl(value_clob, value_char)
          bulk collect into v_ret
          from config_value_char
         where key_value in (select child_id from ancestor
                              where parent_id = p_current_tid
                                and p_xitor_type_id = p_current_tt_id
                                and c_xitor_type_id = p_child_tt_id)
           and config_field_id = p_child_cf_id;

        return v_ret;
    end get_val_clob_collection_child;

    function get_val_date_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_child_cf_id   in config_field.config_field_id%type) return tableofdate
    as
        v_ret tableofdate;
    begin
        select value_date
          bulk collect into v_ret
          from config_value_date
         where key_value in (select child_id from ancestor
                              where parent_id = p_current_tid
                                and p_xitor_type_id = p_current_tt_id
                                and c_xitor_type_id = p_child_tt_id)
           and config_field_id = p_child_cf_id;

        return v_ret;
    end get_val_date_collection_child;

    function get_task_date_collection_child(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_child_tt_id   in xitor_type.xitor_type_id%type,
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return tableofdate
    as
        v_ret tableofdate;
    begin
        select case when is_not_applicable = 1 then null
                    when p_is_start = 1 and p_date_type_id = 0 then t.start_baseline_date
                    when p_is_start = 1 and p_date_type_id = 1 then t.start_projected_date
                    when p_is_start = 1 and p_date_type_id = 2 then t.start_promised_date
                    when p_is_start = 1 and p_date_type_id = 3 then t.start_actual_date
                    when p_is_start = 0 and p_date_type_id = 0 then t.finish_baseline_date
                    when p_is_start = 0 and p_date_type_id = 1 then t.finish_projected_date
                    when p_is_start = 0 and p_date_type_id = 2 then t.finish_promised_date
                    when p_is_start = 0 and p_date_type_id = 3 then t.finish_actual_date
                    else (select case p_is_start 
                                     when 1 then d.start_date
                                     when 0 then d.finish_date
                                 end
                            from wp_task_dates d
                           where d.wp_task_date_type_id = p_date_type_id
                             and d.wp_task_id = t.wp_task_id)
               end
          bulk collect into v_ret
          from ancestor a, wp_workplan wp, wp_tasks t
         where a.parent_id = p_current_tid
           and a.p_xitor_type_id = p_current_tt_id
           and a.c_xitor_type_id = p_child_tt_id
           and a.child_id = wp.xitor_id
           and wp.active = 1
           and wp.wp_workplan_id = t.wp_workplan_id
           and (t.xitor_id = a.child_id or t.parent_xitor_id = a.child_id)
           and wp.template_workplan_id = p_template_workplan_id
           and t.template_task_id = p_template_task_id;

        return v_ret;
    end get_task_date_collection_child;

    function get_val_num_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofnum
    as
        v_ret tableofnum;
    begin
        select value_number
          bulk collect into v_ret
          from config_value_number
         where key_value in (select parent_id from ancestor
                              where child_id = p_current_tid
                                and p_xitor_type_id = p_parent_tt_id
                                and c_xitor_type_id = p_current_tt_id)
           and config_field_id = p_parent_cf_id;

        return v_ret;
    end get_val_num_collection_parent;

    function get_val_char_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofchar
    as
        v_ret tableofchar;
    begin
        select value_char
          bulk collect into v_ret
          from config_value_char
         where key_value in (select parent_id from ancestor
                              where child_id = p_current_tid
                                and p_xitor_type_id = p_parent_tt_id
                                and c_xitor_type_id = p_current_tt_id)
           and config_field_id = p_parent_cf_id;

        return v_ret;
    end get_val_char_collection_parent;

    function get_val_clob_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofclob
    as
        v_ret tableofclob;
    begin
        select nvl(value_clob, value_char)
          bulk collect into v_ret
          from config_value_char
         where key_value in (select parent_id from ancestor
                              where child_id = p_current_tid
                                and p_xitor_type_id = p_parent_tt_id
                                and c_xitor_type_id = p_current_tt_id)
           and config_field_id = p_parent_cf_id;

        return v_ret;
    end get_val_clob_collection_parent;

    function get_val_date_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_parent_cf_id  in config_field.config_field_id%type) return tableofdate
    as
        v_ret tableofdate;
    begin
        select value_date
          bulk collect into v_ret
          from config_value_date
         where key_value in (select parent_id from ancestor
                              where child_id = p_current_tid
                                and p_xitor_type_id = p_parent_tt_id
                                and c_xitor_type_id = p_current_tt_id)
           and config_field_id = p_parent_cf_id;

        return v_ret;
    end get_val_date_collection_parent;

    function get_task_date_collection_parent(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type,
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return tableofdate
    as
        v_ret tableofdate;
    begin
        select case when is_not_applicable = 1 then null
                    when p_is_start = 1 and p_date_type_id = 0 then t.start_baseline_date
                    when p_is_start = 1 and p_date_type_id = 1 then t.start_projected_date
                    when p_is_start = 1 and p_date_type_id = 2 then t.start_promised_date
                    when p_is_start = 1 and p_date_type_id = 3 then t.start_actual_date
                    when p_is_start = 0 and p_date_type_id = 0 then t.finish_baseline_date
                    when p_is_start = 0 and p_date_type_id = 1 then t.finish_projected_date
                    when p_is_start = 0 and p_date_type_id = 2 then t.finish_promised_date
                    when p_is_start = 0 and p_date_type_id = 3 then t.finish_actual_date
                    else (select case p_is_start
                                     when 1 then d.start_date
                                     when 0 then d.finish_date
                                 end
                            from wp_task_dates d
                           where d.wp_task_date_type_id = p_date_type_id
                             and d.wp_task_id = t.wp_task_id)
               end
          bulk collect into v_ret
          from ancestor a, wp_workplan wp, wp_tasks t
         where a.child_id = p_current_tid
           and a.p_xitor_type_id = p_parent_tt_id
           and a.c_xitor_type_id = p_current_tt_id
           and a.parent_id = wp.xitor_id
           and wp.active = 1
           and wp.wp_workplan_id = t.wp_workplan_id
           and (t.xitor_id = a.parent_id or t.parent_xitor_id = a.parent_id)
           and wp.template_workplan_id = p_template_workplan_id
           and t.template_task_id = p_template_task_id;

        return v_ret;
    end get_task_date_collection_parent;

    function get_parent_tid(
        p_current_tid   in xitor.xitor_id%type,
        p_current_tt_id in xitor_type.xitor_type_id%type,
        p_parent_tt_id  in xitor_type.xitor_type_id%type) return xitor.xitor_id%type
    as
        v_ret xitor.xitor_id%type;
    begin
        select parent_id into v_ret 
          from ancestor
         where child_id = p_current_tid
           and p_xitor_type_id = p_parent_tt_id
           and c_xitor_type_id = p_current_tt_id;

        return v_ret;
    exception
        when no_data_found then
            return null;
    end get_parent_tid;

 function get_task_date(
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_trackor_id       in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_date_type_id     in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_start in number) return date
    as
        cursor cur_config_date(
            p_date_type_id in v_wp_task_date_type.wp_task_date_type_id%type,
            p_task_id in wp_tasks.wp_task_id%type) is
                select d.start_date, d.finish_date
                  from wp_task_dates d
                 where d.wp_task_date_type_id = p_date_type_id
                   and d.wp_task_id = p_task_id;

        v_cnt number;
        v_date date;
        v_s_baseline  wp_tasks.start_baseline_date%type;
        v_f_baseline  wp_tasks.finish_baseline_date%type;
        v_s_projected wp_tasks.start_projected_date%type;
        v_f_projected wp_tasks.finish_projected_date%type;
        v_s_actual    wp_tasks.start_actual_date%type;
        v_f_actual    wp_tasks.finish_actual_date%type;
        v_s_promised  wp_tasks.start_promised_date%type;
        v_f_promised  wp_tasks.finish_promised_date%type;
        v_start_date  wp_task_dates.start_date%type;
        v_finish_date wp_task_dates.finish_date%type;
        v_wp_task_id  wp_tasks.wp_task_id%type;
        v_is_na       wp_tasks.is_not_applicable%type;
    begin
        begin
            select t.wp_task_id,
                   t.start_baseline_date,  t.finish_baseline_date,
                   t.start_projected_date, t.finish_projected_date,
                   t.start_actual_date,    t.finish_actual_date,
                   t.start_promised_date,  t.finish_promised_date,
                   t.is_not_applicable
              into v_wp_task_id,
                   v_s_baseline,  v_f_baseline,
                   v_s_projected, v_f_projected,
                   v_s_actual,    v_f_actual,
                   v_s_promised,  v_f_promised,
                   v_is_na
              from wp_tasks t join wp_workplan wp on t.wp_workplan_id = wp.wp_workplan_id
             where (t.xitor_id = p_trackor_id or t.parent_xitor_id = p_trackor_id)
               and wp.template_workplan_id = p_template_workplan_id
               and wp.active  = 1
               and t.template_task_id = p_template_task_id;
        exception
            when no_data_found then
                raise pkg_err_code.e_no_active_workplan;
            when too_many_rows then
                --check for multiple active wp
                select count(1) into v_cnt
                  from wp_workplan
                 where template_workplan_id = p_template_workplan_id
                   and active  = 1
                   and xitor_id = p_trackor_id
                   and rownum <= 2;

                if v_cnt > 1 then 
                    raise pkg_err_code.e_multiple_active_workplans;
                else
                    raise pkg_err_code.e_multiple_child_tasks;
                end if;
            when others then
                raise;
        end;

        if v_is_na = 1 then
            return null;
        end if;

        if p_date_type_id > 3 then
            begin
                select d.start_date, d.finish_date 
                  into v_start_date, v_finish_date
                  from wp_task_dates d
                 where d.wp_task_date_type_id = p_date_type_id
                   and d.wp_task_id = v_wp_task_id;
            exception
                when no_data_found then
                    v_date := null;
            end;
        end if;

        --start date
        if p_is_start = 1 then
            if p_date_type_id = 0 then
                v_date := v_s_baseline;
            elsif p_date_type_id = 1 then
                v_date := v_s_projected;
            elsif p_date_type_id = 2 then
                v_date := v_s_promised;
            elsif p_date_type_id = 3 then
                v_date := v_s_actual;
            else
                v_date := v_start_date;
            end if;

        else --finish date
            if p_date_type_id = 0 then
                v_date := v_f_baseline;
            elsif p_date_type_id = 1 then
                v_date := v_f_projected;
            elsif p_date_type_id = 2 then
                v_date := v_f_promised;
            elsif p_date_type_id = 3 then
                v_date := v_f_actual;
            else
                v_date := v_finish_date;
            end if;
        end if;

        return v_date;
    end get_task_date;

    function is_task_na(
        p_template_workplan_id in wp_workplan.template_workplan_id%type,
        p_trackor_id       in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type) return number
    as
        v_is_na number := 0;

        v_multiple_active_wps exception;
        pragma exception_init(v_multiple_active_wps, -01422);
    begin
        select 1 into v_is_na
          from wp_tasks t join wp_workplan wp on t.wp_workplan_id = wp.wp_workplan_id
         where (t.xitor_id = p_trackor_id or (t.xitor_id is null and t.parent_xitor_id = p_trackor_id))
           and wp.template_workplan_id = p_template_workplan_id
           and wp.active  = 1
           and is_not_applicable = 1
           and t.template_task_id = p_template_task_id;

        return v_is_na;
    exception
        when no_data_found then
            return v_is_na;
        when v_multiple_active_wps then
            raise pkg_err_code.e_multiple_active_workplans;
    end is_task_na;

    function get_file_name_by_blob_id(p_blob_data_id in blob_data.blob_data_id%type) return blob_data.filename%type
    as
        v_filename blob_data.filename%type;
    begin
        select b.filename
          into v_filename
          from blob_data b
         where b.blob_data_id = p_blob_data_id;

        return v_filename;
    exception
        when no_data_found then
            return null;
    end get_file_name_by_blob_id;

    function get_efile_blob_data(p_pk in number, p_cfid in config_field.config_field_id%type) return list_efile_blob
    as
        v_list list_efile_blob;
    begin
        select new t_efile_blob(blob_data_id, filename, blob_data_len)
          bulk collect into v_list
          from blob_data
         where key_value = p_pk
           and config_field_id = p_cfid
         order by line_number;
        return v_list;
    end get_efile_blob_data;

    function exec_linked_value_sql(
        p_entity_id in number,
        p_sql in config_field_linked_value.multiplier_sql_query%type) return number
    as
        v_source_cursor integer;
        v_ret_coef number;
        v_rows number;
    begin
        v_source_cursor := dbms_sql.open_cursor;
        dbms_sql.parse(v_source_cursor, p_sql, dbms_sql.native);

        begin
            dbms_sql.bind_variable(v_source_cursor,'PK', p_entity_id);
        exception when others then null;
        end;


        dbms_sql.define_column(v_source_cursor, 1, v_ret_coef);
        v_rows := dbms_sql.execute_and_fetch(v_source_cursor);
        if v_rows > 0 then
            dbms_sql.column_value(v_source_cursor, 1, v_ret_coef);
        end if;

        dbms_sql.close_cursor(v_source_cursor);

        return v_ret_coef;
    exception
        when others then
            if dbms_sql.is_open(v_source_cursor) then
                dbms_sql.close_cursor(v_source_cursor);
            end if;
        return null;
    end exec_linked_value_sql;

    function get_linked_values_by_entity_id(
        p_cfid in config_field.config_field_id%type,
        p_entity_id in number,
        p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar
    as
        v_ret_values tableofchar := new tableofchar();
        v_cf_value_number number;
    begin
        if p_entity_id is null then
            return v_ret_values;
        end if;

        v_cf_value_number := getValNumNLByID (p_entity_id, p_cfid);
        return get_linked_values_by_cf_value(p_cfid, v_cf_value_number, p_entity_id, p_thousands_separator);
    end get_linked_values_by_entity_id;

    function get_linked_values_by_cf_value(
        p_cfid in config_field.config_field_id%type,
        p_cf_value_number in config_value_number.value_number%type,
        p_entity_id in number,
        p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar
    as
        c_integer_part_num_format constant varchar2(30) := 'FM999999999999999999990';
        c_groupped_integer_part_num_format constant varchar2(30) := 'FM999G999G999G999G999G999G990';
        c_nls_numeric_chars constant varchar2(30) := 'NLS_NUMERIC_CHARACTERS = ''.' || p_thousands_separator || '''';

        v_ret_values tableofchar := new tableofchar();
        v_coef number;
        v_linked_value_num number;
        v_linked_value_char varchar2(500);
        v_number_format varchar2(100);
    begin
        if p_cf_value_number is null then
            return v_ret_values;
        end if;

        for rec in (select v.multiplier_value_number,
                           v.multiplier_sql_query,
                           v.linked_value_type_id,
                           v.num_decimals,
                           v.prefix,
                           v.suffix,
                           f.is_separate_thousands
                      from config_field f
                      join config_field_linked_value v on v.config_field_Id = f.config_field_id
                     where f.config_field_id = p_cfid
                     order by v.order_number) loop
            if rec.linked_value_type_id = c_static_linked_val_type_id then
                v_coef := rec.multiplier_value_number;
            elsif rec.multiplier_sql_query is not null then
                v_coef := exec_linked_value_sql(p_entity_id, rec.multiplier_sql_query);
            end if;

            if v_coef is null then
                v_linked_value_char := c_linked_value_err_msg;
            else
                v_linked_value_num := p_cf_value_number * v_coef;

                if rec.is_separate_thousands = 1 then
                    v_number_format := c_groupped_integer_part_num_format;
                else
                    v_number_format := c_integer_part_num_format;
                end if;

                if rec.num_decimals is not null then
                    v_number_format := v_number_format || 'D' || rpad('0', rec.num_decimals, '0');
                else
                    v_number_format := v_number_format || 'D999999999';
                end if;

                if rec.num_decimals is not null then
                    v_linked_value_num := round(v_linked_value_num, rec.num_decimals);
                    v_linked_value_char := to_char(round(v_linked_value_num, rec.num_decimals), v_number_format, c_nls_numeric_chars);
                else
                    v_linked_value_char := to_char(v_linked_value_num, v_number_format, c_nls_numeric_chars);
                end if;

                --remove decimal separator for integer number: "1234." -> "1234"
                v_linked_value_char := rtrim(v_linked_value_char, '.');

                if rec.prefix is not null then
                    v_linked_value_char := rec.prefix || v_linked_value_char;
                end if;

                if rec.suffix is not null then
                    v_linked_value_char := v_linked_value_char || rec.suffix;
                end if;
            end if;

            v_ret_values.extend();
            v_ret_values(v_ret_values.count) := v_linked_value_char;
        end loop;

        return v_ret_values;
    end get_linked_values_by_cf_value;

    function get_linked_values(
        p_cf_value_numbers in list_config_value_number,
        p_thousands_separator in users.thousands_separator%type default ' ') return list_linked_values_holder
    as
        v_holders list_linked_values_holder;
    begin
        select new t_linked_values_holder(config_field_id,
                                          key_value,
                                          get_linked_values_by_cf_value(config_field_id,
                                                                        value_number,
                                                                        key_value,
                                                                        p_thousands_separator))
          bulk collect into v_holders
          from table(p_cf_value_numbers);

        return v_holders;
    end get_linked_values;

    function get_linked_values_by_drill_down_entity_id(
        p_entity_id in number,
        p_xs_cfid in config_field.config_field_id%type,
        p_drill_down_cfid config_field.config_field_id%type,
        p_thousands_separator in users.thousands_separator%type default ' ') return tableofchar
    as
        v_ret_values tableofchar := new tableofchar();
        v_drill_cf_value_number number;
        v_trackor_selector_value_number number;
    begin
        v_trackor_selector_value_number := getXSkey(p_entity_id, p_xs_cfid, p_drill_down_cfid);
        if v_trackor_selector_value_number is null then
           return v_ret_values;
        end if;

        v_drill_cf_value_number := getValNumNLByID(v_trackor_selector_value_number, p_drill_down_cfid);
        return get_linked_values_by_cf_value(p_drill_down_cfid,
                                             v_drill_cf_value_number,
                                             v_trackor_selector_value_number,
                                             p_thousands_separator);
    end get_linked_values_by_drill_down_entity_id;


    function is_value_in_mult (
        p_pk in config_value_mult.key_value%type,
        p_config_field_id in config_field.config_field_id%type,
        p_value in config_value_mult.value_number%type) return boolean 
    as
        v_count number;
    begin
        select count(1)
          into v_count
          from config_value_mult
         where key_value = p_pk
           and config_field_id = p_config_field_id
           and value_number = p_value;

        if v_count = 0 then
            return false;
        else 
            return true;
        end if;
    end is_value_in_mult;


    function get_str_mult_by_id(
        p_pk in config_value_mult.key_value%type,
        p_config_field_id in config_field.config_field_id%type) return tableofchar 
    as
        v_field_type config_field.data_type%type;
        v_attrib_v_table_id config_field.attrib_v_table_id%type;
        v_value_id_list tableofnum;
        v_value_list tableofchar;
    begin
        begin
            select data_type, attrib_v_table_id
              into v_field_type, v_attrib_v_table_id
              from config_field
             where config_field_id = p_config_field_id
               and data_type in (pkg_cf.c_multi_selector, pkg_cf.c_trackor_multi_selector);
        exception
            when no_data_found then
                raise_application_error(-20000, pkg_label.format(17639, pkg_label.list_label_params('field_id' => p_config_field_id)));
        end;

        v_value_id_list := getAllValNumMultByIDasTabOfNum(p_pk, p_config_field_id);

        if v_field_type = pkg_cf.c_multi_selector then
            select getVTableValue(column_value, v_attrib_v_table_id)
              bulk collect into v_value_list
              from table(v_value_id_list);

        elsif v_field_type = pkg_cf.c_trackor_multi_selector then
            select get_trackor_key(column_value)
              bulk collect into v_value_list
              from table(v_value_id_list);
        end if;

        return v_value_list;
    end get_str_mult_by_id;


    function get_str_mult_by_id_xs(
        p_xs_pk in config_value_mult.key_value%type,
        p_xs_cfid in config_field.config_field_id%type,
        p_cfid config_field.config_field_id%type) return tableofchar
    as
        v_value_list tableofchar;
        v_xs_value_tid config_value_number.value_number%type;
    begin
        v_xs_value_tid := getXSkey(p_xs_pk, p_xs_cfid, p_cfid);
        if v_xs_value_tid is not null then
            v_value_list := get_str_mult_by_id(v_xs_value_tid, p_cfid);
        end if;

        return v_value_list;
    end get_str_mult_by_id_xs;


    function get_task_date(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_template_task_id in wp_tasks.template_task_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date
    as
        v_date date;
    begin
        if p_wp_id is null then
            return null;
        end if;

        if p_dtid <= pkg_wp_template.c_actual_date_type_id then
            begin
                select case when (is_not_applicable = 0 or p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_task_date) 
                            then case when p_dtid = pkg_wp_template.c_baseline_date_type_id  and p_is_finish_date = 0 then  start_baseline_date
                                      when p_dtid = pkg_wp_template.c_baseline_date_type_id  and p_is_finish_date = 1 then finish_baseline_date
                                      when p_dtid = pkg_wp_template.c_projected_date_type_id and p_is_finish_date = 0 then  start_projected_date 
                                      when p_dtid = pkg_wp_template.c_projected_date_type_id and p_is_finish_date = 1 then finish_projected_date
                                      when p_dtid = pkg_wp_template.c_promised_date_type_id  and p_is_finish_date = 0 then  start_promised_date
                                      when p_dtid = pkg_wp_template.c_promised_date_type_id  and p_is_finish_date = 1 then finish_promised_date
                                      when p_dtid = pkg_wp_template.c_actual_date_type_id    and p_is_finish_date = 0 then  start_actual_date
                                      when p_dtid = pkg_wp_template.c_actual_date_type_id    and p_is_finish_date = 1 then finish_actual_date
                                      else null
                                 end
                            when is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_blank       then null
                            when is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_static_date then p_date_value_for_mode_static_date
                            when is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_na          then c_value_for_na
                            else null
                        end
                  into v_date
                  from wp_tasks t
                 where t.wp_workplan_id   = p_wp_id
                   and t.template_task_id = p_template_task_id
                   and (t.xitor_id = p_tid or t.xitor_id is null);
            exception
                when no_data_found then
                    v_date := null;
            end;
        else
            select max(
                   case when (t.is_not_applicable = 0 or p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_task_date)
                        then case when p_is_finish_date = 0
                                  then dt.start_date
                                  else dt.finish_date
                             end
                        when t.is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_blank       then null
                        when t.is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_static_date then p_date_value_for_mode_static_date
                        when t.is_not_applicable = 1 and p_export_na_task_mode = pkg_wp_template.gc_export_na_task_mode_na          then c_value_for_na
                    end)
                    into v_date
              from wp_tasks t, wp_task_dates dt
             where t.wp_workplan_id = p_wp_id
               and t.template_task_id = p_template_task_id
               and dt.wp_task_date_type_id = p_dtid
               and (t.xitor_id = p_tid or t.xitor_id is null)
               and dt.wp_task_id = t.wp_task_id;
        end if;

        return v_date;
    end get_task_date;

    function get_task_date_by_task_name_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_name_id in wp_tasks_name_ids.task_name_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date
    as
        v_template_task_id wp_tasks.template_task_id%type;
    begin
        if p_wp_id is null or p_tid is null or p_task_name_id is null or p_dtid is null then
            return null;
        end if;

        select template_task_id into v_template_task_id
          from wp_tasks_name_ids wptn
          join wp_workplan wp on wp.template_workplan_id = wptn.template_workplan_id
         where wptn.task_name_id = p_task_name_id
           and wp.wp_workplan_id = p_wp_id;

        return get_task_date(
                   p_wp_id => p_wp_id,
                   p_tid   => p_tid,
                   p_template_task_id => v_template_task_id,
                   p_dtid  => p_dtid,
                   p_is_finish_date      => p_is_finish_date,
                   p_export_na_task_mode => p_export_na_task_mode,
                   p_date_value_for_mode_static_date => p_date_value_for_mode_static_date);
    end get_task_date_by_task_name_id;
    
    function get_task_date_by_task_wbs_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_wbs_id in wp_tasks_wbs_ids.wbs_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date
    as
        v_template_task_id wp_tasks.template_task_id%type;
    begin
        if p_wp_id is null or p_tid is null or p_task_wbs_id is null or p_dtid is null then
            return null;
        end if;

        select template_task_id into v_template_task_id
          from wp_tasks_wbs_ids wptn
          join wp_workplan wp on wp.template_workplan_id = wptn.template_workplan_id
         where wptn.wbs_id = p_task_wbs_id
           and wp.wp_workplan_id = p_wp_id;

        return get_task_date(
                   p_wp_id => p_wp_id,
                   p_tid   => p_tid,
                   p_template_task_id => v_template_task_id,
                   p_dtid  => p_dtid,
                   p_is_finish_date      => p_is_finish_date,
                   p_export_na_task_mode => p_export_na_task_mode,
                   p_date_value_for_mode_static_date => p_date_value_for_mode_static_date);
    end get_task_date_by_task_wbs_id;
    
    function get_task_date_by_task_ordnum_id(
        p_wp_id in wp_tasks.wp_workplan_id%type,
        p_tid in wp_tasks.xitor_id%type,
        p_task_ordnum_id in wp_tasks_ordnum_ids.ordnum_id%type,
        p_dtid in v_wp_task_date_type.wp_task_date_type_id%type,
        p_is_finish_date in number,
        p_export_na_task_mode in export_na_task_mode.export_na_task_mode_id%type,
        p_date_value_for_mode_static_date in date default null) return date
    as
        v_template_task_id wp_tasks.template_task_id%type;
    begin
        if p_wp_id is null or p_tid is null or p_task_ordnum_id is null or p_dtid is null then
            return null;
        end if;
        
        select template_task_id into v_template_task_id
          from wp_tasks_ordnum_ids wptn
          join wp_workplan wp on wp.template_workplan_id = wptn.template_workplan_id
         where wptn.ordnum_id = p_task_ordnum_id
           and wp.wp_workplan_id = p_wp_id;

        return get_task_date(
                   p_wp_id => p_wp_id,
                   p_tid   => p_tid,
                   p_template_task_id => v_template_task_id,
                   p_dtid  => p_dtid,
                   p_is_finish_date      => p_is_finish_date,
                   p_export_na_task_mode => p_export_na_task_mode,
                   p_date_value_for_mode_static_date => p_date_value_for_mode_static_date);
    end get_task_date_by_task_ordnum_id;
end pkg_config_field_rpt;
/
