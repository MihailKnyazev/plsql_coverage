CREATE OR REPLACE PACKAGE BODY PKG_AUDIT as

  function get_table_id(p_table_name in audit_log_table.table_name%type)
    return audit_log_table.table_id%type;

  function get_action_id(p_action in audit_log.action%type)
    return audit_log.action_id%type;


  cursor cur_routine_log(
    p_routine_id in audit_call_stack.routine_id%type,
    p_routine_type_id audit_call_stack.routine_type_id%type) is
    select * from (
      select l.table_name, l.action, l.column_name, l.pk, l.audit_log_id, l.to_number, l.line_number, l.from_number, s.routine_id 
        from audit_log l, audit_call_stack s
       where l.audit_log_id = s.audit_log_id 
         and s.routine_type_id = p_routine_type_id and routine_id = p_routine_id
      minus
      select l.table_name, l.action, l.column_name, l.pk, l.audit_log_id, l.to_number, l.line_number, l.from_number, s.routine_id 
        from audit_log l, audit_call_stack s, audit_log_recovery_attempt ra
       where l.audit_log_id = s.audit_log_id 
         and s.routine_type_id = p_routine_type_id and routine_id = p_routine_id
         and ra.audit_log_id = l.audit_log_id and ra.is_recovered = 1)
    order by audit_log_id desc;

  PROCEDURE recover_value(logID number, RecoveryType number)
  as
    tableName varchar2(255);
    field varchar2(255);
    zfield varchar2(255);
    PK number;
    toNumber number;
    toChar varchar2(4000);
    toDate date;
    toBlobID number;
    lineNumber number;
    fieldType number;
    XitorTypeID number;
    objXitorTypeID number;
    fromBlobID number;
    fromNumber number;
    fromChar varchar2(4000);
    x number;
    sdt date;
    fdt date;
    startproj date;
    finishproj date;
    startprom date;
    finishprom date;
    startact date;
    finishact date;
    isna number;
    BlockCalc number;
    dur wp_tasks.duration%type;
    taskwin wp_tasks.task_window%type;
    percent wp_tasks.percent_complete%type;
    flag wp_tasks.task_flag_id%type;
    docname wp_tasks.document_name%type;
    comments wp_tasks.comments%type;
    wbs wp_tasks.wbs%type;
    calendar wp_tasks.wp_calendar_id%type;
    BEGIN
      call_stack_add_routine(4, logID);

      --set default filter values
      for rec in (select table_name, column_name, line_number, pk, from_char, to_char, from_number, to_number, 
                         from_date, to_date, from_blob_data_id, to_blob_data_id, program_id
                    from audit_log 
                   where audit_log_id = logid) loop

        tableName := rec.TABLE_NAME;
        field := rec.COLUMN_NAME;
        zfield := rec.COLUMN_NAME;
        PK := rec.PK;
        lineNumber := rec.LINE_NUMBER;
        if RecoveryType = 0 then
          toNumber := rec.FROM_NUMBER;
          toChar := rec.FROM_CHAR;
          toDate := rec.FROM_DATE;
          toBlobID := rec.FROM_BLOB_DATA_ID;
        else
          toNumber := rec.TO_NUMBER;
          toChar := rec.TO_CHAR;
          toDate := rec.TO_DATE;
          toBlobID := rec.TO_BLOB_DATA_ID;
        end if;

        if tableName = c_tn_cv or tableName = c_tn_trackor or tableName = c_tn_blob_data then
          if tableName = c_tn_trackor then
            for recf in (select c.config_field_id, c.data_type, x.xitor_type_id, x.program_id
                from xitor x, config_field c where x.xitor_id = PK
                    and x.xitor_type_id = c.xitor_type_id and config_field_name = field) loop

              fieldType := recF.DATA_TYPE;

              if field = c_trackor_class_id and toChar is not Null then
                --get ID of xitor class
                begin
                  select xitor_class_id into toNumber from v_xitor_class where xitor_type_id = recF.XITOR_TYPE_ID
                      and (program_id = recF.PROGRAM_ID or program_id is null) and class_name = toChar;
                  fieldType := 1; --Number
                  exception
                  when others then
                  raise_application_error(-20000, '<ERRORMSG>ID value not found for the Trackor Class = "'|| toChar ||'"</ERRORMSG>');
                end;
              end if;
              field := recF.CONFIG_FIELD_ID;
            end loop;
          else
              select data_type, xitor_type_id, obj_xitor_type_id into fieldType, XitorTypeID, objXitorTypeID
                from config_field c 
               where c.config_field_id = field;
          end if;

          if fieldType in (0, 5, 4, 8, 9, 10, 3, 80) then
            --Text, Memo, Lookup, Selector, Checkbox, MultiSelector
            pkg_dl_support.set_cf_data_char(field, PK, toChar, lineNumber, 0);
          elsif fieldType in (20,21) then
            -- Trackor Selector/Drop-dwon
            if toChar is not Null then
              --get ID of xitor object
              begin
                select xitor_id into toNumber from xitor where xitor_id = toNumber;
                exception
                when no_data_found then
                raise_application_error(-20000, '<ERRORMSG>Trackor "'|| toChar ||'" not found</ERRORMSG>');
              end;
            end if;
            pkg_dl_support.set_cf_data_num(field, PK, toNumber, lineNumber, 1);

          elsif fieldType = 1 or fieldType = 11 or fieldType = 12 then
            --Number, Lat, Long
            if zfield = c_trackor_class_id then
              pkg_dl_support.set_cf_data_num(field, PK, toNumber, lineNumber, 1);
            else
              pkg_dl_support.set_cf_data_num(field, PK, toNumber, lineNumber, 0);
            end if;

          elsif fieldType = 2 then
            --Date
            pkg_dl_support.set_cf_data_date(field, PK, toDate, lineNumber, 0);
          elsif fieldType in (90,91) then
            --DateTime
            pkg_dl_support.set_cf_data_num(field, PK, (toDate - TO_DATE('01/01/1970 00:00:00', 'MM-DD-YYYY HH24:MI:SS')) * 24 * 60 * 60 * 1000, lineNumber, 0);
          elsif fieldType = 15 then
              --EFile
              for recf in (select c.value_number, b.filename, dbms_lob.getlength(b.blob_data) ln from config_value_number c, blob_data b
                            where c.config_field_id = field and c.key_value = PK and c.value_number = b.blob_data_id(+))
              loop
                fromBlobID := recF.VALUE_NUMBER;
                fromChar := recF.FILENAME;
                fromNumber := recF.LN; --Blob Size

                --Erase Key_Value and Config_field_id from old BlobID
                update blob_data set key_value=Null, config_field_id=Null where BLOB_DATA_ID = fromBlobID;

                pkg_dl_support.set_cf_data_num(field, PK, toBlobID, 1, 0);
                --Set Key_Value and Config_field_id for the new BlobID
                update blob_data set key_value=PK, config_field_id=field where BLOB_DATA_ID = toBlobID;

                --Log changes
                log_changes(c_tn_blob_data, field, PK, c_la_update, pkg_sec.get_cu(), fromNumber, toNumber, fromChar, toChar, Null, Null, fromBlobID, toBlobID, 1);
              end loop;
          end if;        
        elsif tableName = c_tn_wp_tasks then
          if field = 'PREDECESSOR' then
            pkg_wp.change_preds(PK, toChar);
          elsif field = 'SUCCESSOR' then
            pkg_wp.change_succs(PK, toChar);
          else
            if InStr(field, '_0') > 0 or InStr(field, '_1') > 0 then
              --Configured Date Pair

              x := SUBSTR(field,1,length(field)-2); --wp_task_date_type_id

              --Get current date values
              select t.start_date, t.finish_date 
                into sdt, fdt
                from wp_task_dates t 
               where t.wp_task_id = PK 
                 and t.wp_task_date_type_id = x;

              if SUBSTR(field,length(field),1) = '0' then
                sdt := toDate;
              else
                fdt := toDate;
              end if;
              pkg_wp.update_date_pair(PK, x, sdt, fdt, Null, Null, Null);
            else
              begin
                select start_projected_date, finish_projected_date, start_promised_date, finish_promised_date, start_actual_date,
                  finish_actual_date, is_not_applicable, block_calculations, duration, task_window, percent_complete,
                  task_flag_id, document_name, comments, wbs, wp_calendar_id
                into startproj, finishproj, startprom, finishprom, startact, finishact, isna,
                  BlockCalc, dur, taskwin, percent, flag, docname, comments, wbs, calendar
                from wp_tasks where wp_task_id = PK;
                exception
                when others then
                raise_application_error(-20000, '<ERRORMSG>WP Task not found! Task ID = "'|| PK ||'"</ERRORMSG>');
              end;
              if field = c_td_start_projected_date then
                startproj := toDate;
              elsif field = c_td_finish_projected_date then
                finishproj := toDate;
              elsif field = c_td_start_promised_date then
                startprom := toDate;
              elsif field = c_td_finish_promised_date then
                finishprom := toDate;
              elsif field = c_td_start_actual_date then
                startact := toDate;
              elsif field = c_td_finish_actual_date then
                finishact := toDate;
              elsif field = c_is_not_applicable then
                isna := toNumber;
              elsif field = c_block_calculations then
                BlockCalc := toNumber;
              elsif field = c_duration then
                dur := toNumber;
              elsif field = c_task_window then
                taskwin := toNumber;
              elsif field = 'PERCENT_COMPLETE' then
                percent := toNumber;
              elsif field = c_task_flag_id then
                --get ID of Task Flag
                begin
                  select task_flag_id into flag from v_task_flag where task_flag = toChar;
                  exception
                  when others then
                  raise_application_error(-20000, '<ERRORMSG>ID value not found for the Task Flag = "'|| toChar ||'"</ERRORMSG>');
                end;
              elsif field = c_document_name then
                docname := toChar;
              elsif field = c_comments then
                comments := toChar;
              elsif field = 'WBS' then
                wbs := toChar;
              elsif field = c_wp_calendar_id then
                --get ID of Task Flag
                begin
                  select wp_calendar_id into calendar from wp_calendars
                      where calendar_name = toChar and program_id = rec.program_id;
                  exception
                  when others then
                  raise_application_error(-20000, '<ERRORMSG>ID value not found for the Calendar = "'|| toChar ||'"</ERRORMSG>');
                end;
              end if;
              pkg_wp.update_task(PK, startproj, finishproj, startprom, finishprom, startact, finishact, isna,
                                 BlockCalc, dur, taskwin, percent, flag, docname, comments, wbs, calendar);
            end if;
          end if;
        end if;
      END LOOP;
      call_stack_del_routine(4, logID, null);

      exception
      when others then
      call_stack_del_routine(4, logID, null);
      raise;
    END;


  PROCEDURE recover_object(logID number, recoverWP number, recoverChildren number,
                           resolveCollisions number, parentAttemptID number)
  AS
    c_unable_to_recover_child_t constant varchar2(100) := 'Unable to recover child Trackor (';
    c_not_found constant varchar2(100) := ' not found.';

    attemptID number;
    success number default 0;
    xitorTypeID number;
    progID number;
    PK number;
    newPK number;
    xitorKey varchar2(255);
    isTemplate number;
    templateXitorID number;
    xitorClassID number;
    xitorClass varchar2(255);
    keyPart1 varchar2(100);
    keyPart2 varchar2(100);
    keyPart3 varchar2(100);
    numValues number;
    numBlobs number;
    t1 date;
    t2 date;
    num number;
    ParentID number;
    ChildLogID number;
    msg varchar2(1000);
    BEGIN
      for rec in (select t.program_id, t.pk, t.xitor_type_id, t.xitor_key, t.table_name, la.is_recovered 
                    from audit_log t, audit_log_recovery_attempt la
                   where t.audit_log_id = la.audit_log_id(+) and (la.is_recovered = 0 or la.is_recovered is null)
                     and t.audit_log_id = logID)
      loop
        PK := rec.pk;
        xitorKey := rec.xitor_key;
        if rec.table_name=c_tn_trackor then
          attemptID := log_recovery_attempt(logID);

          begin
            select xt.xitor_type_id into xitorTypeID from xitor_type xt where xt.xitor_type_id = rec.xitor_type_id;
            exception
            when others then begin
            log_recovery_message(attemptID,  3, 'Unable to recover Trackor ('||xitorKey||'): Trackor Type '||rec.xitor_type_id||c_not_found);
            if parentAttemptID is not Null then
              log_recovery_message(parentAttemptID,  2, c_unable_to_recover_child_t||xitorKey||'): Trackor Type '||rec.xitor_type_id||c_not_found);
            end if;
            exit;
          end ;
          end;

          if Nvl(rec.program_id,-1) = -1 then
              log_recovery_message(attemptID,  3, 'Unable to recover Trackor ('||xitorKey||'): The Trackor is Z/P specific, but ZoneID/ProgramID not found in the audit log record.');
              if parentAttemptID is not Null then
                log_recovery_message(parentAttemptID,  2, c_unable_to_recover_child_t||xitorKey||'): The Trackor is Z/P specific, but ZoneID/ProgramID not found in the audit log record.');
              end if;
              exit;
           end if;

            begin
              select program_id into progID from program p
              where p.program_id = rec.program_id;
              exception
              when others then begin
              log_recovery_message(attemptID,  3, 'Unable to recover: Program ID '||rec.program_id||c_not_found);
              if parentAttemptID is not Null then
                log_recovery_message(parentAttemptID,  2, c_unable_to_recover_child_t||xitorKey||'): Program ID '||rec.program_id||c_not_found);
              end if;
              exit;
            end ;
            end;

          pkg_sec.set_pid(progID);

          xitorClass := get_last_char_value(c_tn_trackor, c_trackor_class_id, PK); --String value of Xitor Class
          if Nvl(xitorClass, '0') <> '0' then
            begin
              select xitor_class_id into xitorclassid from v_xitor_class where upper(class_name) = upper(xitorclass) and
                  PROGRAM_ID = progID and XITOR_TYPE_ID = rec.xitor_type_id;
              exception
              when others then begin
              log_recovery_message(attemptID,  3, 'Unable to recover: Trackor Class ('||xitorClass||') not found.');
              if parentAttemptID is not Null then
                log_recovery_message(parentAttemptID,  2, c_unable_to_recover_child_t||xitorKey||'): Trackor Class ('||xitorClass||') not found.');
              end if;
              exit;
            end ;
            end;
          end if;

          isTemplate := get_last_number_value(c_tn_trackor, 'IS_TEMPLATE', PK);
          templateXitorID := get_last_number_value(c_tn_trackor, 'TEMPLATE_XITOR_ID', PK);
          keyPart1 := get_last_char_value(c_tn_trackor, 'AUTO_KEY_PART1', PK);
          keyPart2 := get_last_char_value(c_tn_trackor, 'AUTO_KEY_PART2', PK);
          keyPart3 := get_last_char_value(c_tn_trackor, 'AUTO_KEY_PART3', PK);

          /*
          if resolveCollisions = 1 then
             --Check if this xitor key already exists
             if isZpSpecific = 1 then
                Select Count(xitor_key) into numValues from XITOR Where Upper(xitor_key) = Upper(xitorKey) and
                     PROGRAM_ID = progID and XITOR_TYPE_ID = rec.xitor_type_id;
             else
                Select Count(xitor_key) into numValues from XITOR Where Upper(xitor_key) = Upper(xitorKey) and
                     PROGRAM_ID is null and XITOR_TYPE_ID = rec.xitor_type_id;
             end if;
             if numValues > 1 then
                xitorKey := xitorKey || '#';
             end if;
          end if;
          */

          begin
            --create Xitor
            begin
              insert into XITOR(xitor_type_id, xitor_key, is_template,
                                template_xitor_id, program_id, xitor_class_id)
              values(rec.xitor_type_id, xitorKey, isTemplate,
                     templateXitorID, progID, xitorClassID) returning xitor_id into newPK;

              if keyPart3 is not Null then
                insert into XITOR_AUTO_KEY(xitor_id, auto_key_part1, auto_key_part2, auto_key_part3)
                values(newPK, keyPart1, keyPart2, keyPart3);
              end if;
              exception
              when others then begin
              if resolveCollisions = 1 then
                xitorKey := xitorKey || '#';
                insert into XITOR(xitor_type_id, xitor_key, is_template,
                                  template_xitor_id, program_id, xitor_class_id)
                values(rec.xitor_type_id, xitorKey, isTemplate,
                       templateXitorID, progID, xitorClassID) returning xitor_id into newPK;
                if keyPart3 is not Null then
                  insert into XITOR_AUTO_KEY(xitor_id, auto_key_part1, auto_key_part2, auto_key_part3)
                  values(newPK, keyPart1, keyPart2, keyPart3);
                end if;
              else
                RAISE;
              end if;
            end;
            end;

            --copy all config values from log to new xitor
            numValues := 0;
            numValues := recover_config_fields(PK, newPK);
            exception
            when others then begin
            log_recovery_message(attemptID,  3, 'Unable to recover: Cannot create Trackor.'||chr(10)||chr(13)||sqlerrm);
            if parentAttemptID is not Null then
              log_recovery_message(parentAttemptID,  2, c_unable_to_recover_child_t||xitorKey||'): Cannot create Trackor.'||chr(10)||chr(13)||sqlerrm);
            end if;
            rollback;
            exit;
          end ;
          end;

          --Restore Picture Library blobs
          update blob_data b set
            b.remove_after = null, b.key_value = newPK
          where key_value = PK and config_field_id is null;
          select count(*) into numBlobs
          from blob_data b where key_value = newPK and config_field_id is null;


          msg := 'Trackor ('|| xitorKey ||') successfully recovered: New PKey is '||to_char(newPK)||'.';
          if numValues > 0 then
            msg := msg ||chr(10)||chr(13)|| to_char(numValues)||' config values successfully recovered.';
          end if;
          if numBlobs > 0 then
            msg := msg ||chr(10)||chr(13)|| to_char(numBlobs)||' Picture Library photos successfully recovered.';
          end if;
          log_recovery_message(attemptID,  1, msg);
          success := 1;

          if parentAttemptID is not Null then
            msg := 'Child Trackor ('|| xitorKey ||') successfully recovered: New PKey is '||to_char(newPK)||'.';
            if numValues > 0 then
              msg := msg ||chr(10)||chr(13)|| to_char(numValues)||' config values successfully recovered.';
            end if;
            log_recovery_message(parentAttemptID,  1, msg);
          end if;

          if success = 1 then
            update audit_log_recovery_attempt a set a.is_recovered = 1 where a.audit_log_recovery_attempt_id = attemptID;
            update audit_log a set a.new_pk = newPK where a.audit_log_id = logID;
          end if;


          --
          --Set relation among current Xitor and all possible Parents
          --

          --First, get the time frame of the last Delete Relation operation
          --ACTION_ID=3 - Delete Operation
          select max(ts), max(ts)-(30/(24*3600)) into t1, t2 from audit_log t
          where t.table_name=c_tn_relation and ACTION_ID=3 and t.PK = rec.pk;

          if t1 is not null then
            for recrel in (
                select from_number, from_char, line_number 
                  from audit_log t 
                 where t.table_name = c_tn_relation and action_id = 3
                   and t.PK = rec.pk and t.TS <= t1 and t.TS> = t2 
                 order by t.audit_log_id asc) loop

              --Check that 'ParentXitorID' exists in the DB
              ParentID := recRel.From_Number;
              select count(*) into num from XITOR where XITOR_ID = ParentID;
              if num = 0 then
                --The Parent Xitor was deleted, we need to make sure that is was recovered later and we need to get new XITOR_ID
                begin
                  select new_pk into parentid from audit_log a where a.table_name=c_tn_trackor
                      and a.action_id=3 and a.pk = recRel.From_Number;
                  exception
                  when others then
                  ParentID := Null;
                  log_recovery_message(attemptID,  2, 'Unable to create Relation with parent('||recRel.From_Char||').'||chr(10)||chr(13)||'Parent Trackor not found.');
                  if parentAttemptID is not Null then
                    log_recovery_message(parentAttemptID,  2, 'Unable to create Relation with parent('||recRel.From_Char||').'||chr(10)||chr(13)||'Parent Trackor not found.');
                  end if;
                end;
              end if;

              if ParentID is not Null then
                begin
                  --Set Relations with parents
                  pkg_relation.new_relation(ParentID, newPK, recRel.Line_Number);
                  exception
                  when others then
                  log_recovery_message(attemptID,  2, 'Unable to create Relation with parent('||recRel.From_Char||').'||chr(10)||chr(13)||sqlerrm);
                  if parentAttemptID is not Null then
                    log_recovery_message(parentAttemptID,  2, 'Unable to create Relation with parent('||recRel.From_Char||').'||chr(10)||chr(13)||sqlerrm);
                  end if;
                end;
              end if;
            end loop;
          end if;

          --
          --Set relation among current Xitor and all possible Children (the children that were not deleted)
          --

          --First, get the time frame of the last Delete Relation operation
          --ACTION_ID=3 - Delete Operation
          select max(ts), max(ts)-(30/(24*3600)) into t1, t2 from audit_log t
          where t.table_name=c_tn_relation and ACTION_ID=3 and t.from_number = rec.pk;

          if t1 is not null then
            for recrel in (select pk, line_number, from_char 
                             from audit_log t 
                            where t.table_name = c_tn_relation and action_id = 3
                              and t.from_number = rec.pk and t.ts <= t1 
                              and t.ts >= t2 
                            order by t.audit_log_id asc) loop

                --Check that 'Child' exists
                select count(*) into num from XITOR where XITOR_ID = recRel.PK;

                if num <> 0 then
                   begin
                     --Set Relations with child
                     pkg_relation.new_relation(newPK, recRel.Pk, recRel.Line_Number);
                   exception
                   when others then
                     log_recovery_message(attemptID,  2, 'Unable to create Relation with child('||recRel.From_Char||').'||chr(10)||chr(13)||sqlerrm);
                     if parentAttemptID is not Null then
                        log_recovery_message(parentAttemptID,  2, 'Unable to create Relation with child('||recRel.From_Char||').'||chr(10)||chr(13)||sqlerrm);
                     end if;
                   end;
                end if;
            end loop;
          end if;

          --Add records to BLOB_ANCESTOR for the new BLOBs (Single E-Files)
          for recBlob in (select blob_data_id, key_value from blob_data b where b.key_value = newPK) loop
              pkg_dl_support.update_blob_ancestor(recBlob.blob_data_id, recBlob.Key_Value);
          end loop;

          if recoverChildren = 1 and success = 1 then
            --Find children and recover all of them if they are not recovered yet
            --ACTION_ID=3 - Delete Operation
            select max(ts), max(ts)-(30/(24*3600)) into t1, t2 from audit_log t
            where t.table_name=c_tn_relation and ACTION_ID=3 and t.FROM_NUMBER = rec.pk;

            if t1 is not null then
              for recrel in (select pk from audit_log t 
                              where t.table_name = c_tn_relation and action_id = 3
                                and t.from_number = rec.pk and t.ts <= t1 and t.ts >= t2 
                              order by t.audit_log_id asc) loop

                --Check that 'ChildXitorID' is deleted and get AuditLogID for it
                ChildLogID := Null;
                begin
                  select audit_log_id into ChildLogID from audit_log a where a.table_name=c_tn_trackor
                      and a.action_id=3 and a.pk = recRel.PK and a.new_pk is null;
                  exception
                  when others then
                  ChildLogID := Null;
                end;

                if ChildLogID is not Null then
                  if parentAttemptID is not Null then
                    recover_object(ChildLogID, recoverWP, recoverChildren, resolveCollisions, parentAttemptID);
                  else
                    recover_object(ChildLogID, recoverWP, recoverChildren, resolveCollisions, AttemptID);
                  end if;
                end if;
              end loop;
            end if;
          end if;


          if recoverWP = 1 and success = 1 then
            --First, get the time frame of the last Delete Relation operation
            --ACTION_ID=3 - Delete Operation
            select max(TS), max(TS)-(60/(24*3600)) into t1, t2 from audit_log t
            where t.table_name=c_tn_wp_workplan and ACTION_ID=3 and t.xitor_id = rec.pk;

            --Recover all recently deleted Workplans of this Trackor
            for recwps in (select t.audit_log_id from audit_log t where t.table_name=c_tn_wp_workplan and action_id=3
                and t.xitor_id=rec.pk and t.TS <= t1 and t.TS>=t2 order by t.audit_log_id asc)
            loop
              recover_wp(recWPs.audit_log_id, attemptID, parentAttemptID);
            end loop;
          end if;
        elsif rec.table_name=c_tn_wp_workplan then
          --Recover WORKPLAN
          recover_wp(logID, Null, Null);
        end if;
      end loop;
    END;


  PROCEDURE recover_wp(logID number, XitorAttemptID number, parentAttemptID number)
  AS
    c_unable_to_recover_wp constant varchar2(100) :='Unable to recover WP(';

    WPAttemptID number;
    oldWPID number;
    newWPID number;
    numValues number;
    XitorID number;
    XitorKey varchar2(500);
    XitorTypeID number;
    TemplateWorkplanID number;
    WPName varchar2(500);
    WPStart date;
    WPActive number;
    zoneID number;
    programID number;
    num number;
    TaskXitorID number;
    TaskName varchar2(500);
    CFDPStart date;
    CFDPFinish date;
    WpTaskDateTypeID number;
    oldVal varchar2(200);
    newVal varchar2(200);
    columnName varchar2(100);
    msg varchar2(1000);
    p_rule_return_str varchar2(2000);

    PROCEDURE check_and_log(PK number, col VARCHAR2, oldc VARCHAR2, newc VARCHAR2) AS
      action VARCHAR2(10);
      v_table_id audit_log_table.table_id%type;
      v_action_id audit_log.action_id%type;
      BEGIN
        IF oldc is not Null THEN
          action := c_la_update;
        ELSE
          action := c_la_insert;
        END IF;

        v_table_id := get_table_id(c_tn_wp_tasks);
        v_action_id := get_action_id(action);
        INSERT INTO AUDIT_LOG (TABLE_NAME, table_id, COLUMN_NAME, PK, CASCADE_PK, FROM_CHAR, TO_CHAR,ACTION, action_id, TS, USER_ID)
        VALUES (c_tn_wp_tasks, v_table_id, col,PK,Null,oldc,newc,action,v_action_id,current_date,Pkg_Sec.GET_CU);
      END;

  begin
      for rec in (select t.*, la.is_recovered 
                    from audit_log t, audit_log_recovery_attempt la
                   where t.audit_log_id = la.audit_log_id(+) and (la.is_recovered = 0 or la.is_recovered is null)
                     and t.audit_log_id = logid)
      loop
        WPAttemptID := log_recovery_attempt(logID);

        numValues := 0;
        oldWPID := rec.pk;
        XitorID := rec.xitor_id;
        XitorKey := rec.xitor_key;
        XitorTypeID := rec.xitor_type_id;
        zoneID := rec.zone_id;
        programID := rec.program_id;

        pkg_sec.set_pid(programID);

        TemplateWorkplanID := get_last_number_value(c_tn_wp_workplan, 'TEMPLATE_WORKPLAN_ID', oldWPID);
        WPName := get_last_char_value(c_tn_wp_workplan, 'NAME', oldWPID);
        WPStart := get_last_date_value(c_tn_wp_workplan, 'WORKPLAN_START', oldWPID);
        WPActive := get_last_number_value(c_tn_wp_workplan, 'ACTIVE', oldWPID);

        --Check that 'XitorID' exists in the DB
        select count(*) into num from XITOR where XITOR_ID = XitorID;
        if num = 0 then
          --The Xitor was deleted, we need to make sure that is was recovered later and we need to get new XITOR_ID
          --ACTION_ID=3 - Delete Operation
          begin
            select new_pk into xitorid from audit_log a where a.table_name=c_tn_trackor
                and a.action_id=3 and a.pk = XitorID;
            exception
            when others then
            log_recovery_message(WPAttemptID, 3, c_unable_to_recover_wp||WPName||'): Cannot find Trackor (ID = '||to_char(XitorID)||', Key = '|| XitorKey);
            if XitorAttemptID is not Null then
              log_recovery_message(XitorAttemptID, 2, c_unable_to_recover_wp||WPName||'): Cannot find Trackor (ID = '||to_char(XitorID)||', Key = '|| XitorKey);
            end if;
            if parentAttemptID is not Null then
              log_recovery_message(parentAttemptID, 2, c_unable_to_recover_wp||WPName||'): Cannot find Trackor (ID = '||to_char(XitorID)||', Key = '|| XitorKey);
            end if;
            rollback;
            exit;
          end;
        end if;

        --Create initial Workplan for a Xitor
        begin
          --insert into WP_WORKPLAN(workplan_type, template_workplan_id, workplan_start, name, active, xitor_type_id, xitor_id)
          --values(1, TemplateWorkplanID, WPStart, WPName, WPActive, XitorTypeID, XitorID) returning wp_workplan_id into newWPID;
          newWPID := pkg_wp.assign_wp(TemplateWorkplanID, XitorID, WPName, WPStart, null, WPActive, p_rule_return_str);
        exception
          when others then
          log_recovery_message(WPAttemptID,  3, c_unable_to_recover_wp||WPName||')'||chr(10)||chr(13)||sqlerrm);
          if XitorAttemptID is not Null then
            log_recovery_message(XitorAttemptID,  2, c_unable_to_recover_wp||WPName||')'||chr(10)||chr(13)||sqlerrm);
          end if;
          if parentAttemptID is not Null then
            log_recovery_message(parentAttemptID,  2, c_unable_to_recover_wp||WPName||')'||chr(10)||chr(13)||sqlerrm);
          end if;
          rollback;
          exit;
        end;


        --Copy task dates and other fields from old workplan's tasks
        begin
          for recTask in (select * from wp_tasks t where t.wp_workplan_id = newWPID)
          loop
            TaskXitorID := recTask.Xitor_Id; --new xitor id
            TaskName := recTask.Task_Name;

            if Nvl(TaskXitorID,-1) <> -1 then
              if Nvl(XitorAttemptID,0) = 0 then
                select count(*) into num from XITOR where XITOR_ID = TaskXitorID;
              else
                num := 0;
              end if;
            else
              num := 1;
            end if;

            begin
                if num = 0 then
                    --Try get old XITOR_ID from audit_log
                    --ACTION_ID=3 - Delete Operation
                    select pk into taskxitorid 
                      from audit_log a 
                     where a.table_name = c_tn_trackor
                       and a.action_id = 3 and a.new_pk = XitorID;
                end if;
            exception
                when others then taskxitorid := null;
            end;

            --Copy task dates and other fields from old workplan's tasks
            --ACTION_ID=3 - Delete Operation
            for rectlog in (select from_date, from_number, from_char, program_id, column_name
                              from audit_log a 
                             where a.table_name = c_tn_wp_tasks and a.template_task_id = recTask.template_task_id
                               and a.wp_workplan_id = oldwpid and a.action_id = 3 
                               and ((a.xitor_id = taskxitorid) or (a.xitor_id is null and taskxitorid is null)))
            loop
              columnName := upper(recTLog.column_name);
              if columnName = c_td_start_early_date and Nvl(recTask.START_EARLY_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_EARLY_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_early_date, recTask.START_EARLY_DATE, recTLog.From_Date);
              elsif columnName = c_td_start_baseline_date and Nvl(recTask.START_BASELINE_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_BASELINE_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_baseline_date, recTask.START_BASELINE_DATE, recTLog.From_Date);
              elsif columnName = c_td_start_late_date and Nvl(recTask.START_LATE_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_LATE_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_late_date, recTask.START_LATE_DATE, recTLog.From_Date);
              elsif columnName = c_td_start_projected_date and Nvl(recTask.START_PROJECTED_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_PROJECTED_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_projected_date, recTask.START_PROJECTED_DATE, recTLog.From_Date);
              elsif columnName = c_td_start_promised_date and Nvl(recTask.START_PROMISED_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_PROMISED_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_promised_date, recTask.START_PROMISED_DATE, recTLog.From_Date);
              elsif columnName = c_td_start_actual_date and Nvl(recTask.START_ACTUAL_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set START_ACTUAL_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_start_actual_date, recTask.START_ACTUAL_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_early_date and Nvl(recTask.FINISH_EARLY_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_EARLY_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_early_date, recTask.FINISH_EARLY_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_baseline_date and Nvl(recTask.FINISH_BASELINE_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_BASELINE_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_baseline_date, recTask.FINISH_BASELINE_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_late_date and Nvl(recTask.FINISH_LATE_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_LATE_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_late_date, recTask.FINISH_LATE_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_projected_date and Nvl(recTask.FINISH_PROJECTED_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_PROJECTED_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_projected_date, recTask.FINISH_PROJECTED_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_promised_date and Nvl(recTask.FINISH_PROMISED_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_PROMISED_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_promised_date, recTask.FINISH_PROMISED_DATE, recTLog.From_Date);
              elsif columnName = c_td_finish_actual_date and Nvl(recTask.FINISH_ACTUAL_DATE,current_date) <> Nvl(recTLog.From_Date,current_date) then
                update wp_tasks set FINISH_ACTUAL_DATE = recTLog.From_Date where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_td_finish_actual_date, recTask.FINISH_ACTUAL_DATE, recTLog.From_Date);
              elsif columnName = c_is_not_applicable and Nvl(recTask.IS_NOT_APPLICABLE,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set IS_NOT_APPLICABLE = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_is_not_applicable, recTask.IS_NOT_APPLICABLE, recTLog.From_Number);
              elsif columnName = 'ON_TIME_START' and Nvl(recTask.ON_TIME_START,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set ON_TIME_START = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, 'ON_TIME_START', recTask.ON_TIME_START, recTLog.From_Number);
              elsif columnName = 'ON_TIME_FINISH' and Nvl(recTask.ON_TIME_FINISH,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set ON_TIME_FINISH = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, 'ON_TIME_FINISH', recTask.ON_TIME_FINISH, recTLog.From_Number);
              elsif columnName = c_duration and Nvl(recTask.DURATION,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set DURATION = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_duration, recTask.DURATION, recTLog.From_Number);
              elsif columnName = c_task_window and Nvl(recTask.TASK_WINDOW,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set TASK_WINDOW = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_task_window, recTask.TASK_WINDOW, recTLog.From_Number);
              elsif columnName = c_buffer_value and Nvl(recTask.BUFFER_VALUE,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set BUFFER_VALUE = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_buffer_value, recTask.BUFFER_VALUE, recTLog.From_Number);
              elsif columnName = c_discp_id and Nvl(recTask.DISCP_ID,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set DISCP_ID = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_discp_id, recTask.DISCP_ID, recTLog.From_Number);
              elsif columnName = c_comments and Nvl(recTask.COMMENTS,'*') <> Nvl(recTLog.From_Char,'*') then
                update wp_tasks set COMMENTS = recTLog.From_Char where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_comments, recTask.COMMENTS, recTLog.From_Char);
              elsif columnName = c_task_flag_id and Nvl(recTask.TASK_FLAG_ID,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set TASK_FLAG_ID = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                begin
                  select task_flag into oldVal from v_task_flag where task_flag_id=recTask.TASK_FLAG_ID;
                  exception
                  when others then oldVal := to_char(recTask.TASK_FLAG_ID);
                end;
                begin
                  select task_flag into newVal from v_task_flag where task_flag_id=recTLog.From_Number;
                  exception
                  when others then newVal := to_char(recTLog.From_Number);
                end;
                check_and_log(recTask.wp_task_id, c_task_flag_id, oldVal, newVal);
              elsif columnName = c_document_name and Nvl(recTask.DOCUMENT_NAME,'*') <> Nvl(recTLog.From_Char,'*') then
                update wp_tasks set DOCUMENT_NAME = recTLog.From_Char where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_document_name, recTask.DOCUMENT_NAME, recTLog.From_Char);
              elsif columnName = c_wp_calendar_id and Nvl(recTask.WP_CALENDAR_ID,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set WP_CALENDAR_ID = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                begin
                  select calendar_name into oldval from wp_calendars where wp_calendar_id=rectask.wp_calendar_id
                      and program_id = rectask.program_id;
                  exception
                  when others then oldVal := to_char(recTask.WP_CALENDAR_ID);
                end;
                begin
                  select calendar_name into newval from wp_calendars where wp_calendar_id=recTlog.from_number
                      and program_id = recTlog.program_id;
                  exception
                  when others then newVal := to_char(recTLog.From_Number);
                end;
                check_and_log(recTask.wp_task_id, c_wp_calendar_id, oldVal, newVal);
              elsif columnName = 'WBS' and Nvl(recTask.WBS,'*') <> Nvl(recTLog.From_Char,'*') then
                update wp_tasks set WBS = recTLog.From_Char where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, 'WBS', recTask.WBS, recTLog.From_Char);
              elsif columnName = c_block_calculations and Nvl(recTask.BLOCK_CALCULATIONS,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set BLOCK_CALCULATIONS = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, c_block_calculations, recTask.BLOCK_CALCULATIONS, recTLog.From_Number);
              elsif columnName = 'IS_REQUIRED' and Nvl(recTask.IS_REQUIRED,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set IS_REQUIRED = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, 'IS_REQUIRED', recTask.IS_REQUIRED, recTLog.From_Number);
              elsif columnName = 'IS_MILESTONE' and Nvl(recTask.IS_MILESTONE,-1) <> Nvl(recTLog.From_Number,-1) then
                update wp_tasks set IS_MILESTONE = recTLog.From_Number where wp_task_id = recTask.wp_task_id;
                check_and_log(recTask.wp_task_id, 'IS_MILESTONE', recTask.IS_MILESTONE, recTLog.From_Number);
              end if;
            end loop;


            for recTLog in (select distinct PK from audit_log a where a.table_name = c_tn_wp_tasks and a.template_task_id = recTask.template_task_id
                and a.wp_workplan_id = oldwpid and a.action_id = 3 and ((a.xitor_id = taskxitorid) or (a.xitor_id is null and taskxitorid is null)))
            loop
              --Recover all config values for this Task
              begin
                numValues := numValues + recover_config_fields(recTLog.PK, recTask.Wp_Task_Id);
                exception
                when others then null;
              end;

              --Recover old Preds/Succs lists for this Task (if Preds/Succs were changed)
              --It is enough to recover either Pred or Succ list, I decided to recover Successors.
              --First step: get Successors for the current task (if exists)
              for recsucc in (select to_char from audit_log a where a.table_name = c_tn_wp_tasks and action = c_la_update
                  and a.column_name = 'SUCCESSOR' and a.pk = recTLog.PK order by ts desc)
              loop
                begin
                  pkg_wp.change_succs(recTask.Wp_Task_Id, recSucc.to_char);
                  exception
                  when others then
                  log_recovery_message(WPAttemptID,  2, 'Unable to change Successors.'||chr(10)||chr(13)||'WP: '||WPName||'.'||chr(10)||chr(13)||'Task Name:'||TaskName||'.'||chr(10)||chr(13)||'Successors: '||rec.to_char||'.'||chr(10)||chr(13)||'ERROR:'||sqlerrm);
                  if XitorAttemptID is not Null then
                    log_recovery_message(XitorAttemptID,  2, 'Unable to change Successors.'||chr(10)||chr(13)||'WP: '||WPName||'.'||chr(10)||chr(13)||'Task Name:'||TaskName||'.'||chr(10)||chr(13)||'Successors: '||rec.to_char||'.'||chr(10)||chr(13)||'ERROR:'||sqlerrm);
                  end if;
                  if parentAttemptID is not Null then
                    log_recovery_message(parentAttemptID,  2, 'Unable to change Successors.'||chr(10)||chr(13)||'WP: '||WPName||'.'||chr(10)||chr(13)||'Task Name:'||TaskName||'.'||chr(10)||chr(13)||'Successors: '||rec.to_char||'.'||chr(10)||chr(13)||'ERROR:'||sqlerrm);
                  end if;
                end;
                exit;
              end loop;


              --Recover CFDPs for this Task
              --ACTION_ID=3 - Delete Operation
              for reccfdp in (select distinct template_task_id from audit_log a where a.table_name = 'WP_TASK_DATES'
                  and a.action_id = 3 and a.pk = recTLog.pk and a.From_Date is not Null)
              loop
                WpTaskDateTypeID := recCFDP.Template_Task_Id; --This is WP_TASK_DATE_TYPE_ID

                begin
                    select max(case when upper(column_name) = 'START_DATE' then from_date
                                    else null
                                end),
                           max(case when upper(column_name) = 'FINISH_DATE' then from_date
                                    else null
                                end)
                      into CFDPStart, CFDPFinish
                      from audit_log a 
                     where a.table_name = 'WP_TASK_DATES'
                       and a.action_id = 3 and a.pk = recTLog.pk 
                       and a.from_date is not Null and a.Template_Task_Id = WpTaskDateTypeID
                       and a.column_name in ('START_DATE','FINISH_DATE');
                exception
                    when no_data_found then
                        CFDPStart := null;
                        CFDPFinish := null;
                end;


                if CFDPStart is not Null or CFDPFinish is not Null then
                  begin
                    insert into wp_task_dates(wp_task_id, wp_task_date_type_id, start_date, finish_date)
                    values(recTask.Wp_Task_Id, WpTaskDateTypeID, CFDPStart, CFDPFinish);

                    if CFDPStart is not Null then
                      check_and_log(recTask.wp_task_id, WpTaskDateTypeID||'_0', Null, CFDPStart);
                    end if;

                    if CFDPFinish is not Null then
                      check_and_log(recTask.wp_task_id, WpTaskDateTypeID||'_1', Null, CFDPFinish);
                    end if;
                    exception
                    when others then Null;
                  end;
                end if;
              end loop;

            end loop;
          end loop;
        exception
            when others then
               log_recovery_message(WPAttemptID,  3, c_unable_to_recover_wp||WPName||'): Cannot copy fields for the Task('|| TaskName ||').'||chr(10)||chr(13)||sqlerrm);

               if XitorAttemptID is not Null then
                   log_recovery_message(XitorAttemptID,  2, c_unable_to_recover_wp||WPName||'): Cannot copy fields for the Task('|| TaskName ||').'||chr(10)||chr(13)||sqlerrm);
               end if;
               if parentAttemptID is not Null then
                   log_recovery_message(parentAttemptID,  2, c_unable_to_recover_wp||WPName||'): Cannot copy fields for the Task('|| TaskName ||').'||chr(10)||chr(13)||sqlerrm);
               end if;
               rollback;
               exit;
        end;

        --Recover all config values of this WP
        numValues := numValues + recover_config_fields(oldWPID, newWPID);

        msg := 'Workplan ('|| WPName ||') successfully recovered: New WORKPLAN_ID is '||to_char(newWPID)||'.';
        if numValues > 0 then
          msg := msg ||chr(10)||chr(13)|| to_char(numValues)||' config values successfully recovered.';
        end if;
        log_recovery_message(WPAttemptID,  1, msg);
        if XitorAttemptID is not Null then
          log_recovery_message(XitorAttemptID,  1, msg);
        end if;
        if parentAttemptID is not Null then
          log_recovery_message(parentAttemptID,  1, msg);
        end if;

        update audit_log_recovery_attempt a set a.is_recovered = 1 where a.audit_log_recovery_attempt_id = WPAttemptID;
        update audit_log a set a.new_pk = newWPID where a.audit_log_id = logID; --Store new WP_ID
      end loop;

  END;

  FUNCTION log_recovery_attempt(auditLogID number) return number
  AS
      PRAGMA AUTONOMOUS_TRANSACTION;
      ID number;
  BEGIN
      INSERT INTO audit_log_recovery_attempt (audit_log_id, ts, is_recovered)
      VALUES (auditLogID, current_date, 0) returning audit_log_recovery_attempt_id into ID;
      COMMIT;
      return ID;
  END;

  PROCEDURE log_recovery_message(attemptID number,  msgTypeID number,msg VARCHAR2)
  AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
      INSERT INTO audit_log_recovery_msg (audit_log_recovery_attempt_id, audit_log_msg_type_id, message)
      VALUES (attemptID, msgTypeID, msg);
      COMMIT;
    END;

  FUNCTION get_last_number_value(tableName varchar2, columnName varchar2, Pkey number) return number
  AS
    ret number;
    BEGIN
      for rec in (select to_number from audit_log a where a.table_name = tablename and action <> c_la_delete
          and a.column_name = columnName and a.pk = PKey order by ts desc)
      loop
        ret := rec.to_number;
        exit;
      end loop;
      return ret;
    END;

  FUNCTION get_last_char_value(tableName varchar2, columnName varchar2, Pkey number) return varchar2
  AS
    ret varchar2(4000);
    BEGIN
      for rec in (select to_char from AUDIT_LOG a where a.table_name = tableName and action <> c_la_delete
          and a.column_name = columnname and a.pk = pkey order by ts desc)
      loop
        ret := rec.to_char;
        exit;
      end loop;
      return ret;
    END;

  FUNCTION get_last_date_value(tableName varchar2, columnName varchar2, Pkey number) return date
  AS
    ret date;
    BEGIN
      for rec in (select to_date from audit_log a where a.table_name = tablename and action <> c_la_delete
          and a.column_name = columnName and a.pk = PKey order by ts desc)
      loop
        ret := rec.to_date;
        exit;
      end loop;
      return ret;
    END;

  FUNCTION recover_config_fields(oldPKey number, newPkey number) return number
  AS
    numValues number default 0;
    fieldID number;
    BEGIN
      for rec in (select t.audit_log_id, f.config_field_id, t.column_name, t.from_number,
                    t.from_char, t.from_date, b.blob_data_id, t.line_number, f.data_type, t.ts, f.xitor_type_id, f.is_lockable,
                    f.attrib_v_table_id, f.program_id
                  from audit_log t, config_field f, blob_data b,
                    (select max(l.audit_log_id) as audit_log_id from audit_log l where l.pk=oldPKey
                       and (l.table_name=c_tn_cv or l.table_name=c_tn_blob_data) and  l.action_id=3
                    group by l.pk,l.table_name,l.column_name, l.line_number) m
                  where
                    t.audit_log_id = m.audit_log_id and t.column_name=f.config_field_id
                    and t.from_blob_data_id=b.blob_data_id(+))
      loop
          begin
              if rec.data_type in (1,3,4,8,9,10,11,12,20,21) then
                --Number, Lat, Long, Checkbox, Lookups
                insert into config_value_number(key_value, config_field_id, value_number)
                values(newPKey, to_number(rec.column_name), rec.from_number);
              elsif rec.data_type = 80 then
                --MultiSelector
                if rec.from_char is not null then
                   pkg_dl_support.set_cf_data_char(to_number(rec.column_name), newPKey, rec.from_char, 1, 0);
                end if;
              elsif rec.data_type in (0,30) then
                --Text, Hyperlink
                insert into config_value_char(key_value, config_field_id, value_char)
                values(newPKey, to_number(rec.column_name), rec.from_char);
              elsif rec.data_type in (5,7) then
                --Memo, Rich Memo
                insert into config_value_clob(key_value, config_field_id, value_clob)
                values(newPKey, to_number(rec.column_name), rec.from_char);
              elsif rec.data_type in (2,90,91) then
                --Date
                insert into config_value_date(key_value, config_field_id, value_date)
                values(newPKey, to_number(rec.column_name), rec.from_date);
              elsif rec.data_type = 15 and rec.blob_data_id is not null then
                --EFile
                  insert into config_value_number(key_value, config_field_id, value_number)
                  values(newPKey, to_number(rec.column_name), rec.blob_data_id) returning config_field_id into fieldID;

                  update blob_data b set b.key_value = newPKey, b.config_field_id = fieldID, b.remove_after = null
                  where b.blob_data_id = rec.blob_data_id;

                  log_changes(c_tn_blob_data, fieldID, newPKey, c_la_insert, pkg_sec.get_cu(), Null, rec.from_number, Null, rec.from_char, Null, Null, Null, rec.blob_data_id, 1);
              end if;
          exception
              when others then 
                  null;
          end;

          if rec.is_lockable = 1 then
            --Recover Lock state
            insert into config_field_lock(key_value, config_field_id, user_id)
                select newPKey, to_number(t.column_name), t.user_id
                  from audit_log t, config_field f,
                       (select max(l.audit_log_id) audit_log_id 
                          from audit_log l 
                         where l.pk = oldPKey
                           and l.table_name = 'CONFIG_FIELD_LOCK'
                         group by l.pk, l.table_name, l.column_name, l.line_number) m
                 where t.audit_log_id = m.audit_log_id 
                   and t.column_name = f.config_field_id
                   and t.column_name = rec.column_name;
          end if;
        numValues := numValues + 1;
      end loop;

      return numValues;
    END;


  procedure recover_routine_value(p_rec in cur_routine_log%rowtype) as
    v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
    v_errmsg varchar2(2000);
    begin
      v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

      pkg_audit.recover_value(p_rec.audit_log_id, 0);

      update audit_log_recovery_attempt set is_recovered = 1
      where audit_log_recovery_attempt_id = v_attempt_id;
      exception
      when others then
      v_errmsg := sqlerrm;
      log_recovery_message(v_attempt_id,  3, 'Unable to recover: ' || v_errmsg);
    end recover_routine_value;


  procedure recover_routine_xitor(p_rec in cur_routine_log%rowtype) as
    v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
    v_errmsg varchar2(2000);
    begin
      if (p_rec.action = c_la_insert and p_rec.column_name = 'XITOR_KEY') then
        v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

        pkg_xitor.drop_xitor(p_rec.pk);

        update audit_log_recovery_attempt set is_recovered = 1
        where audit_log_recovery_attempt_id = v_attempt_id;

      elsif (p_rec.action = c_la_update) then
        v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

        recover_value(p_rec.audit_log_id, 0);

        update audit_log_recovery_attempt set is_recovered = 1
        where audit_log_recovery_attempt_id = v_attempt_id;

      elsif (p_rec.action = c_la_delete) then
        v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

        recover_object(p_rec.audit_log_id, 1, 1, 1, null);

        update audit_log_recovery_attempt set is_recovered = 1
        where audit_log_recovery_attempt_id = v_attempt_id;
      end if;
      exception
      when others then
      v_errmsg := sqlerrm;
      log_recovery_message(v_attempt_id,  3, 'Unable to recover xitor: '
                                             || v_errmsg);
    end recover_routine_xitor;


  procedure recover_routine_relation(p_rec in cur_routine_log%rowtype) as
    v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
    v_parent_exist integer;
    v_pid number;
    v_errmsg varchar2(2000);
    begin
      v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

      if (p_rec.action = c_la_insert) then
        pkg_relation.del_relation(p_rec.to_number, p_rec.pk, p_rec.line_number);
      elsif (p_rec.action = c_la_delete) then
        select count(*) into v_parent_exist
        from xitor where xitor_id = p_rec.from_number;

        if (v_parent_exist = 0) then
          select new_pk into v_pid
          from audit_log l, audit_call_stack s
          where l.audit_log_id = s.audit_log_id
                and s.routine_type_id = 2 and routine_id = p_rec.routine_id
                and l.pk = p_rec.from_number and l.table_name = c_tn_trackor
                and l.action = c_la_delete;
        else
          v_pid := p_rec.from_number;
        end if;

        pkg_relation.new_relation(v_pid, p_rec.pk, p_rec.line_number);
      end if;

      update audit_log_recovery_attempt set is_recovered = 1
      where audit_log_recovery_attempt_id = v_attempt_id;
    exception
        when others then
        v_errmsg := sqlerrm;
        log_recovery_message(v_attempt_id,  3, 'Unable to recover relation: ' || v_errmsg);
    end recover_routine_relation;


  procedure recover_routine_lock(p_rec in cur_routine_log%rowtype) as
      v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
      v_errmsg varchar2(2000);
  begin
      v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

      if (p_rec.action = c_la_insert) then
          delete from config_field_lock where key_value = p_rec.pk
             and config_field_id = p_rec.column_name;

          delete from config_field_lock_mult
           where key_value = p_rec.pk and line_number = p_rec.line_number
             and config_field_id = p_rec.column_name;
      elsif (p_rec.action = c_la_delete) then
          --TODO whoud we recover deleted locks?
          null;
      end if;

      update audit_log_recovery_attempt set is_recovered = 1
       where audit_log_recovery_attempt_id = v_attempt_id;
  exception
      when others then
          v_errmsg := sqlerrm;
          log_recovery_message(v_attempt_id,  3, 'Unable to recover lock: ' || v_errmsg);
  end recover_routine_lock;


  procedure recover_routine_wp(p_rec in cur_routine_log%rowtype) as
    v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
    v_errmsg varchar2(2000);
    begin
      v_attempt_id := log_recovery_attempt(p_rec.audit_log_id);

      -- only support WP deletion for now
      if (p_rec.action = c_la_insert) then
        delete from wp_workplan where wp_workplan_id = p_rec.pk;

        update audit_log_recovery_attempt set is_recovered = 1
        where audit_log_recovery_attempt_id = v_attempt_id;
      else
        log_recovery_message(v_attempt_id,  2, 'Unable to recover, not supported action');
      end if;

      exception
      when others then
      v_errmsg := sqlerrm;
      log_recovery_message(v_attempt_id,  3, 'Unable to recover WP: '
                                             || v_errmsg);
    end recover_routine_wp;


  procedure recover_routine(
    p_routine_id in audit_call_stack.routine_id%type,
    p_routine_type_id audit_call_stack.routine_type_id%type) as

    v_attempt_id audit_log_recovery_attempt.audit_log_recovery_attempt_id%type;
    begin
        for rec in cur_routine_log(p_routine_id, p_routine_type_id) loop
            case rec.table_name
                when c_tn_cv then
                   recover_routine_value(rec);
                when c_tn_blob_data then
                   recover_routine_value(rec);
                when c_tn_trackor then
                   recover_routine_xitor(rec);
                when c_tn_relation then
                   recover_routine_relation(rec);
                when c_tn_wp_tasks then
                   recover_routine_value(rec);
                when 'CONFIG_FIELD_LOCK' then
                   recover_routine_lock(rec);
                when c_tn_wp_workplan then
                   recover_routine_wp(rec);
            else
                v_attempt_id := log_recovery_attempt(rec.audit_log_id);
                log_recovery_message(v_attempt_id,  2, 'Unable to recover, not supported table');
            end case;
            commit;
        end loop;
    end recover_routine;


  procedure recover_import(proc_id in process.process_id%type) as
    v_imp_name imp_spec.name%type;
    v_imp_spec_id imp_spec.imp_spec_id%type;
    v_imp_run_id imp_run.imp_run_id%type;
  begin
      select s.imp_spec_id, s.name, r.imp_run_id into v_imp_spec_id, v_imp_name, v_imp_run_id
      from imp_spec s, imp_run r
      where r.imp_spec_id = s.imp_spec_id and r.process_id = proc_id;

      call_stack_add_routine(6, v_imp_run_id, v_imp_spec_id);

      update process set status_id = 11 where process_id = proc_id;

      recover_routine(v_imp_run_id, 2);

      call_stack_del_routine(6, v_imp_run_id, v_imp_spec_id);
      update process set status_id = 12 where process_id = proc_id;
  exception
      when others then
          call_stack_del_routine(6, v_imp_run_id, v_imp_spec_id);
          update process set status_id = 12 where process_id = proc_id;
          raise;
  end recover_import;


  procedure recover_rule(p_rrid in rule_run.rule_run_id%type) as
    v_rule_name rule.rule%type;
    v_rule_id rule.rule_id%type;
    v_process_id process.process_id%type;
  begin
      select r.rule, r.rule_id, rr.process_id into v_rule_name, v_rule_id, v_process_id
      from rule r, rule_run rr where r.rule_id = rr.rule_id and rr.rule_run_id = p_rrid;

      call_stack_add_routine(8, p_rrid, v_rule_id);
      update process set status_id = 11 where process_id = v_process_id;

      recover_routine(p_rrid, 7);

      call_stack_del_routine(8, p_rrid, v_rule_id);
      update process set status_id = 12 where process_id = v_process_id;
  exception
      when others then
      call_stack_del_routine(8, p_rrid, v_rule_id);
      update process set status_id = 12 where process_id = v_process_id;
      raise;
  end recover_rule;

  procedure log_changes(
    tablename in varchar2,
    field in varchar2,
    pkey in number,
    action in varchar2,
    user_id in varchar2,
    from_number in number,
    to_number in number,
    from_char in varchar2,
    to_char in varchar2,
    from_date in date,
    to_date in date,
    from_blob_data_id in number,
    to_blob_data_id in number,
    linenumber in number,
    prog_id in number default null,
    xt_id in number default null)
  is
  begin
      log_changes(tablename,field,pkey,action,user_id,from_number,to_number,from_char,to_char,from_date,to_date,from_blob_data_id,to_blob_data_id,linenumber,null,prog_id,xt_id,null);
  end;


  procedure log_changes(
    tablename in varchar2,
    field in varchar2,
    pkey in number,
    action in varchar2,
    user_id in varchar2,
    from_number in number,
    to_number in number,
    from_char in varchar2,
    to_char in varchar2,
    from_date in date,
    to_date in date,
    from_blob_data_id in number,
    to_blob_data_id in number,
    linenumber in number,
    fieldcode in varchar2,
    prog_id in number default null,
    xt_id in number default null,
    newpk in number default null)
  is
    v_audit_log_id audit_log.audit_log_id%type;
    v_table_id audit_log_table.table_id%type;
    v_action_id audit_log.action_id%type;
  begin
      if not pkg_audit.disable_audit_log then
          v_audit_log_id := seq_audit_log_id.nextval;

          if tablename in (c_tn_trackor, c_tn_cv, c_tn_blob_data) 
              and field is not null 
              and pkey is not null 
              and action in (c_la_insert, c_la_update, c_la_delete) then

              insert into audit_log_cflite(column_name, pk, ts, user_id, audit_log_id, program_id)
                  values(case field
                            when 'XITOR_KEY' then '0'
                            when c_trackor_class_id then '4'
                            else field 
                         end,
                         pkey, current_date, nvl(user_id, 0), v_audit_log_id, prog_id);
          elsif tablename = 'USERS' and field is not null and pkey is not null and action in (c_la_insert, c_la_update) then

              insert into audit_log_cflite(column_name, pk, ts, user_id, audit_log_id, program_id)
                  values(case field 
                            when 'IS_DISABLED' then '3'
                            when 'UN' then '5'
                            when 'EMAIL' then '6'
                            else field
                         end, 
                         to_number(nvl(fieldcode, 0)), current_date, nvl(user_id, 0), v_audit_log_id, prog_id);
          end if;

          v_table_id := get_table_id(tablename);
          v_action_id := get_action_id(action);

          insert into audit_log
              (audit_log_id, table_name, table_id, column_name, pk, action,
               action_id, user_id, from_number, to_number, from_char, to_char, from_date,
               to_date, from_blob_data_id, to_blob_data_id, line_number, program_id, xitor_type_id, ts, field_code, new_pk)
          values (v_audit_log_id, tablename, v_table_id, field, pkey, action,
                  v_action_id, user_id, from_number, to_number, from_char, to_char, from_date,
                  to_date, from_blob_data_id, to_blob_data_id, linenumber, prog_id, xt_id, current_date, fieldcode, newpk);
      end if;
  end log_changes;


  procedure log_task_changes(
    p_table_name in audit_log.table_name%type,
    p_column_name audit_log.column_name%type,
    p_pk audit_log.pk%type,
    p_action audit_log.action%type,
    p_wpid audit_log.wp_workplan_id%type,
    p_template_task_id audit_log.template_task_id%type,
    p_xtid audit_log.xitor_type_id%type,
    p_xid audit_log.xitor_id%type,
    p_from_number audit_log.from_number%type,
    p_to_number audit_log.to_number%type,
    p_from_char audit_log.from_char%type,
    p_to_char audit_log.to_char%type,
    p_from_date audit_log.from_date%type,
    p_to_date audit_log.to_date%type)
  is
    v_table_id audit_log_table.table_id%type;
    v_action_id audit_log.action_id%type;
  begin
      if not pkg_audit.disable_audit_log then
          v_table_id := get_table_id(p_table_name);
          v_action_id := get_action_id(p_action);

          insert into audit_log (table_name, table_id, column_name, pk, action,
                                 action_id, wp_workplan_id, template_task_id, xitor_type_id, xitor_id,
                                 from_number, to_number, from_char, to_char, from_date, to_date, field_code)
          values ( p_table_name, v_table_id, p_column_name, p_pk, p_action,
                                 v_action_id, p_wpid, p_template_task_id, p_xtid, p_xid,
                                 p_from_number, p_to_number, p_from_char, p_to_char, p_from_date, p_to_date, 't_'||to_char(p_template_task_id));
      end if;
  end log_task_changes;


  function get_table_id(p_table_name in audit_log_table.table_name%type)
      return audit_log_table.table_id%type
  is
      v_table_id audit_log_table.table_id%type;
  begin
      select table_id into v_table_id
        from audit_log_table where table_name = p_table_name;

      return v_table_id;
  end get_table_id;


  function get_action_id(p_action in audit_log.action%type)
    return audit_log.action_id%type
  is
    v_action_id audit_log.action_id%type;
  begin
      case p_action
        when c_la_update then v_action_id := 1;
        when c_la_insert then v_action_id := 2;
        when c_la_delete then v_action_id := 3;
        when 'DL' then v_action_id := 4;
        when 'A' then v_action_id := 5;
        when 'BDL' then v_action_id := 6; --Bulk Download
        when 'RDL' then v_action_id := 7; --Report Download
      else raise_application_error(-20000, '<ERRORMSG>Not supported action [' || p_action || ']</ERRORMSG>');
      end case;
      return v_action_id;
  end get_action_id;


  procedure log_task_changes_full(
    OldTask wp_tasks%rowtype,
    NewTask wp_tasks%rowtype,
    CascadePK number,
    LType audit_log.action%type)
  as
    oldVal varchar2(4000);
    newVal varchar2(4000);

    procedure check_and_log(col varchar2, oldc varchar2, newc varchar2) as
      diff boolean;
      oldc2 varchar2(4000);
      begin

        oldc2 := oldc;
        if ltype = c_la_insert then
          oldc2 := null;
        end if;

        if (oldc2 is null and newc is not null) then
          diff := true;
        elsif (oldc2 is not null and newc is null) then
          diff := true;
        elsif (nvl(oldc2, '-1') <> nvl(newc,'-1')) then
          diff := true;
        else
          diff := false;
        end if;

        if diff then
          log_task_updates(
              p_column_name => col,
              p_pk => NewTask.WP_TASK_ID,
              p_action => LType,
              p_cascade_pk => CascadePK,
              p_from_number => null,
              p_to_number => null,
              p_from_char => oldc2,
              p_to_char => newc,
              p_from_date => null,
              p_to_date => null,
              p_field_code => 't_'||to_char(OldTask.template_task_id));
        end if;
      end check_and_log;

    procedure check_and_log(col varchar2, oldc number, newc number) as
      diff boolean;
      oldc2 number;
      begin

        oldc2 := oldc;
        if LType = c_la_insert then
          oldc2 := null;
        end if;

        if (oldc2 is null and newc is not null) then
          diff := true;
        elsif (oldc2 is not null and newc is null) then
          diff := true;
        elsif (nvl(oldc2,-1) <> nvl(newc,-1)) then
          diff := true;
        else
          diff := false;
        end if;

        if diff then
          log_task_updates(
              p_column_name => col,
              p_pk => NewTask.WP_TASK_ID,
              p_action => LType,
              p_cascade_pk => CascadePK,
              p_from_number => oldc2,
              p_to_number => newc,
              p_from_char => null,
              p_to_char => null,
              p_from_date => null,
              p_to_date => null,
              p_field_code => 't_'||to_char(OldTask.template_task_id));
        end if;
      end check_and_log;

    procedure check_and_log(col varchar2, oldc date, newc date) as
      diff boolean;
      oldc2 date;
      begin

        oldc2 := oldc;
        if LType = c_la_insert then
          oldc2 := null;
        end if;

        if (oldc2 is null and newc is not null) then
          diff := true;
        elsif (oldc2 is not null and newc is null) then
          diff := true;
        elsif (nvl(oldc2, current_date-1) <> nvl(newc, current_date-1)) then
          diff := true;
        else
          diff := false;
        end if;

        if diff then
          log_task_updates(
              p_column_name => col,
              p_pk => NewTask.WP_TASK_ID,
              p_action => LType,
              p_cascade_pk => CascadePK,
              p_from_number => null,
              p_to_number => null,
              p_from_char => null,
              p_to_char => null,
              p_from_date => oldc2,
              p_to_date => newc,
              p_field_code => 't_'||to_char(OldTask.template_task_id));

        end if;
      end check_and_log;

  begin
      if not pkg_audit.disable_audit_log then
          if upper(ltype) in (c_la_delete, 'DELETE') then
              log_task_updates(
                  p_column_name => null,
                  p_pk => newtask.wp_task_id,
                  p_action => ltype,
                  p_cascade_pk  => cascadepk,
                  p_from_number => null,
                  p_to_number  => null,
                  p_from_char  => null,
                  p_to_char    => null,
                  p_from_date  => null,
                  p_to_date    => null,
                  p_field_code => 't_'||to_char(oldtask.template_task_id));
          else
              check_and_log('BACKWARD_RANK', oldtask.backward_rank, newtask.backward_rank);
              check_and_log('BASELINE_DURATION', oldtask.baseline_duration, newtask.baseline_duration);
              check_and_log('BASELINE_TASK_WINDOW', oldtask.baseline_task_window, newtask.baseline_task_window);
              check_and_log(c_block_calculations, nvl(oldtask.block_calculations, 0), nvl(newtask.block_calculations, 0));
              check_and_log(c_buffer_value, oldtask.buffer_value, newtask.buffer_value);
              check_and_log(c_comments, oldtask.comments, newtask.comments);
              check_and_log('CONSTRAINT_DATE', oldtask.constraint_date, newtask.constraint_date);
              check_and_log('DEADLINE_DATE', oldtask.deadline_date, newtask.deadline_date);
              check_and_log(c_discp_id, oldtask.discp_id, newtask.discp_id);
              check_and_log(c_document_name, oldtask.document_name, newtask.document_name);
              check_and_log(c_duration, oldtask.duration, newtask.duration);
              check_and_log(c_td_finish_actual_date, oldtask.finish_actual_date, newtask.finish_actual_date);
              check_and_log(c_td_finish_baseline_date, oldtask.finish_baseline_date, newtask.finish_baseline_date);
              check_and_log(c_td_finish_early_date, oldtask.finish_early_date, newtask.finish_early_date);
              check_and_log(c_td_finish_late_date, oldtask.finish_late_date, newtask.finish_late_date);
              check_and_log(c_td_finish_projected_date, oldtask.finish_projected_date, newtask.finish_projected_date);
              check_and_log(c_td_finish_promised_date, oldtask.finish_promised_date, newtask.finish_promised_date);
              check_and_log('FORWARD_RANK', oldtask.forward_rank, newtask.forward_rank);
              check_and_log('HAS_DETAIL_ITEMS', oldtask.has_detail_items, newtask.has_detail_items);
              check_and_log(c_is_not_applicable, nvl(oldtask.is_not_applicable,0), nvl(newtask.is_not_applicable, 0));
              check_and_log('ON_TIME_FINISH', oldtask.on_time_finish, newtask.on_time_finish);
              check_and_log('ON_TIME_START', oldtask.on_time_start, newtask.on_time_start);
              check_and_log('ORDER_NUMBER', oldtask.order_number, newtask.order_number);
              check_and_log('OUTLINE_NUMBER', oldtask.outline_number, newtask.outline_number);
              check_and_log('PERCENT_COMPLETE', oldtask.percent_complete, newtask.percent_complete);

              begin
                  select calendar_name into oldval from wp_calendars
                   where wp_calendar_id = oldtask.wp_calendar_id;
              exception
                  when others then
                     oldval := oldtask.wp_calendar_id;
              end;

              begin
                  select calendar_name into newval from wp_calendars
                   where wp_calendar_id = newtask.wp_calendar_id;
              exception
                  when others then
                      newval := newtask.wp_calendar_id;
              end;

              check_and_log(c_wp_calendar_id, oldval, newval);
              check_and_log('WP_WORKPLAN_ID', oldtask.wp_workplan_id, newtask.wp_workplan_id);
              check_and_log('WP_TASK_ID', oldtask.wp_task_id, newtask.wp_task_id);
              check_and_log(c_td_start_actual_date, oldtask.start_actual_date, newtask.start_actual_date);
              check_and_log(c_td_start_baseline_date, oldtask.start_baseline_date, newtask.start_baseline_date);
              check_and_log(c_td_start_early_date ,oldtask.start_early_date, newtask.start_early_date);
              check_and_log(c_td_start_late_date, oldtask.start_late_date, newtask.start_late_date);
              check_and_log(c_td_start_projected_date, oldtask.start_projected_date, newtask.start_projected_date);
              check_and_log(c_td_start_promised_date, oldtask.start_promised_date, newtask.start_promised_date);

              begin
                  select task_flag into oldval from v_task_flag
                   where task_flag_id = oldtask.task_flag_id;
              exception
                  when others then
                      oldval := oldtask.task_flag_id;
              end;

              begin
                  select task_flag into newval from v_task_flag
                   where task_flag_id = newtask.task_flag_id;
              exception
                  when others then
                      newval := newtask.task_flag_id;
              end;

              check_and_log(c_task_flag_id, oldval, newval);
              check_and_log('TASK_NAME', oldtask.task_name, newtask.task_name);
              check_and_log(c_task_window, oldtask.task_window, newtask.task_window);
              check_and_log('TEMPLATE_TASK_ID', oldtask.template_task_id, newtask.template_task_id);
              check_and_log('TOTAL_SLACK', oldtask.total_slack, newtask.total_slack);
              check_and_log('XITOR_TYPE_ID', oldtask.xitor_type_id, newtask.xitor_type_id);
              check_and_log('XITOR_ID', oldtask.xitor_id, newtask.xitor_id);
              check_and_log('TS', oldtask.ts,newtask.ts);
              check_and_log('UN', oldtask.un,newtask.un);
              check_and_log('WBS', oldtask.wbs,newtask.wbs);
              check_and_log('WORK_CONTENT', oldtask.work_content, newtask.work_content);

              if ltype = c_la_insert then
                  newval := pkg_wp_template.predessor_list(newtask.wp_task_id);
                  if nvl(newval,'-1') <> '-1' then
                      check_and_log('PREDECESSOR', null, newval);
                  end if;

                  newval := pkg_wp_template.successor_list(newtask.wp_task_id);
                  if nvl(newval,'-1') <> '-1' then
                      check_and_log('SUCCESSOR', null, newval);
                  end if;
              end if;
          end if;    
      end if;  

  end log_task_changes_full;

  procedure log_wp_deletion(
    p_pk in audit_log.pk%type,
    p_from_char in audit_log.from_char%type,
    p_xid in audit_log.xitor_id%type)
  is
    v_xkey xitor.xitor_key%type;
    v_xtid xitor.xitor_type_id%type;
    v_pid xitor.program_id%type;
    v_table_id audit_log_table.table_id%type;
    v_action_id audit_log.action_id%type;
  begin
      if not pkg_audit.disable_audit_log then
          select xitor_key, xitor_type_id, program_id
            into v_xkey, v_xtid, v_pid
            from xitor where xitor_id = p_xid;

          v_table_id := get_table_id(c_tn_wp_workplan);
          v_action_id := get_action_id(c_la_delete);
          insert into audit_log(table_name, table_id, pk, action, action_id,
                                from_char, xitor_id, xitor_key, xitor_type_id, program_id)
          values (c_tn_wp_workplan, v_table_id, p_pk, c_la_delete, v_action_id,
                  p_from_char, p_xid, v_xkey, v_xtid, v_pid);
      end if;
  end log_wp_deletion;


  procedure log_xitor_deletion(
    p_pk in audit_log.pk%type,
    p_xtid in audit_log.xitor_type_id%type,
    p_xkey in audit_log.xitor_key%type,
    p_pid in audit_log.program_id%type,
    p_field_code in audit_log.field_code%type)
  is
    v_table_id audit_log_table.table_id%type;
    v_action_id audit_log.action_id%type;
  begin
      if not pkg_audit.disable_audit_log then
          v_table_id := get_table_id(c_tn_trackor);
          v_action_id := get_action_id(c_la_delete);

          insert into audit_log(table_name, table_id, pk, action,
                                action_id, xitor_key, xitor_type_id, program_id, field_code)
          values (c_tn_trackor, v_table_id, p_pk, c_la_delete,
                  v_action_id, p_xkey, p_xtid, p_pid, p_field_code);
      end if;
  end log_xitor_deletion;


  procedure log_task_updates(
    p_column_name audit_log.column_name%type,
    p_pk audit_log.pk%type,
    p_action audit_log.action%type,
    p_cascade_pk audit_log.cascade_pk%type,
    p_from_number audit_log.from_number%type,
    p_to_number audit_log.to_number%type,
    p_from_char audit_log.from_char%type,
    p_to_char audit_log.to_char%type,
    p_from_date audit_log.from_date%type,
    p_to_date audit_log.to_date%type,
    p_field_code audit_log.field_code%type)
  is
    v_table_id audit_log_table.table_id%type;
    v_action_id audit_log.action_id%type;
  begin
      if not pkg_audit.disable_audit_log then
          v_table_id := get_table_id(c_tn_wp_tasks);
          v_action_id := get_action_id(p_action);

          insert into audit_log (table_name, table_id, column_name, pk, action, action_id, cascade_pk,
                                 from_number, to_number, from_char, to_char, from_date, to_date, field_code)
          values (c_tn_wp_tasks, v_table_id, p_column_name, p_pk, p_action, v_action_id, p_cascade_pk,
                              p_from_number, p_to_number, p_from_char, p_to_char, p_from_date, p_to_date, p_field_code);
      end if;
  end log_task_updates;


  procedure log_multiple_relation(cid number, rtid number) as
      ids varchar2(2000);
      keys varchar2(2000);
  begin
      if not pkg_audit.disable_audit_log then
          for rec in (
              select t.parent_id, pkg_config_field_rpt.getValStrByStaticID(x.xitor_id, f.config_field_id) as xitor_key
                from relation t, xitor x, config_field f
               where t.child_id = cid and t.relation_type_id = rtid
                 and x.xitor_type_id = f.xitor_type_id
                 and f.is_static = 1 and f.config_field_name = 'XITOR_KEY'
                 and t.parent_id = x.xitor_id /*order by t.ts*/) loop

              ids := ids || rec.parent_id || ', ';
              keys := keys || rec.xitor_key || '; ';
          end loop;

          keys := substr(keys, 1, length(keys)-2);
          ids := substr(ids, 1, length(ids)-2);

          pkg_audit.log_changes('RELATION_MULT',       -- tablename
                                'PARENT_ID',           -- field
                                cid,                   -- pkey
                                c_la_insert,                   -- action
                                pkg_sec.get_cu(),      -- user_id
                                Null,                  -- from_number
                                Null,                  -- to_number
                                ids,                   -- from_char
                                keys,                  -- to_char
                                Null,                  -- from_date
                                Null,                  -- to_date
                                Null,                  -- from_blob_data_id
                                Null,                  -- to_blob_data_id
                                rtid);                 -- linenumber
      end if;                            
  exception
      when others then
          raise_application_error(-20000, 'Error when logging multiple relation');
  end log_multiple_relation;


  procedure call_stack_add_routine(
      p_routine_type_id in audit_call_stack_temp.routine_type_id%type,
      p_routine_id in audit_call_stack_temp.routine_id%type,
      p_routine2_id in audit_call_stack_temp.routine2_id%type default null,
      p_routine_text in audit_call_stack_temp.routine_text%type default null,
      p_routine_start_time in audit_call_stack_temp.routine_start_time%type default null) 
  is
      pragma autonomous_transaction;
      v_cnt number;
  begin
      if not pkg_audit.disable_audit_log then
          if (p_routine_id is null and p_routine_type_id in (4, 3, 10, 11, 12, 13)) then
              select count(*) into v_cnt from audit_call_stack_temp;
              if (v_cnt = 1) then
                  --manual routine already set in call stack
                  return;
              end if;
          end if;

          insert into audit_call_stack_temp(routine_type_id, routine_id, routine2_id, routine_text, routine_start_time)
            values(p_routine_type_id, p_routine_id, p_routine2_id, p_routine_text, nvl(p_routine_start_time, current_date));

          commit;
      end if;      
  end call_stack_add_routine;


  procedure call_stack_del_routine(
      p_routine_type_id in audit_call_stack_temp.routine_type_id%type,
      p_routine_id in audit_call_stack_temp.routine_id%type,
      p_routine2_id in audit_call_stack_temp.routine2_id%type default null) 
  is
      pragma autonomous_transaction;
  begin
      if not pkg_audit.disable_audit_log then
          delete from audit_call_stack_temp
           where routine_type_id = p_routine_type_id
             and ((routine_id = p_routine_id) or (p_routine_id is null and routine_type_id in (4, 3, 10, 11, 12, 13, 16)))
             and ((p_routine2_id is null and routine2_id is null) or
                  (p_routine2_id is not null and p_routine2_id = routine2_id));

          commit;
      end if;
  end call_stack_del_routine;


  procedure call_stack_del_routine_all is
      pragma autonomous_transaction;
  begin
      if not pkg_audit.disable_audit_log then
          delete from audit_call_stack_temp;
          commit;
      end if;      
  end call_stack_del_routine_all;


  procedure set_routine_id(p_audit_log_id in audit_log.audit_log_id%type) 
  is
      pragma autonomous_transaction;
      v_cnt number;
  begin
      if not pkg_audit.disable_audit_log then
          select count(*) into v_cnt from audit_call_stack_temp;
          if (v_cnt = 1) then --Manual update always executed 1st ...
              update audit_call_stack_temp set routine_id = p_audit_log_id
               where routine_type_id in (4, 3, 10, 11, 12, 13, 16);
          else  --but in some cases this update is not logged in ADUTO_LOG, for example - change of account settings may trigger Rule, which changes CF
              update audit_call_stack_temp set routine_id = p_audit_log_id
               where routine_type_id in (4, 3, 10, 11, 12, 13, 16) and routine_id is null;  

          end if;
          commit;
      end if;
  end set_routine_id;


  function get_last_audit_log_id return audit_log.audit_log_id%type 
  is
      v_audit_log_id audit_log.audit_log_id%type;
  begin
      v_audit_log_id := audit_log_id;
      audit_log_id := null;

      return v_audit_log_id;
  end get_last_audit_log_id;

end;
/