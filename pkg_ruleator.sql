CREATE OR REPLACE PACKAGE BODY PKG_RULEATOR as

    procedure no_direct_rule_mods as
        begin
            if not allow_rule_mods then
                raise_application_error (-20000, 'Do not modify the table directly. Use pkg_ruleator instead.');
            end if;
        end no_direct_rule_mods;

    function delete_rule(p_rule_id rule.rule_id%type) return number
    as
        v_rule rule%rowtype;
        begin
            allow_rule_mods := true;

            delete from rule where rule_id = p_rule_id;

            allow_rule_mods := false;
            return sql%rowcount;
        end delete_rule;

    procedure delete_rule(p_rule_id rule.rule_id%type)
    as
        v_ignored number;
        begin
            v_ignored := delete_rule(p_rule_id);
        end delete_rule;

    /**
     * Actually executes rule and return value of :RETURN_STR
     */
    function exec(
        p_rule_id in rule.rule_id%type,
        p_id_field in rule_type.id_field%type,
        p_id in number,
        p_id_num number default null,
        p_id2 in number default null)
        return varchar2;

    /*Deprecated*/
    function execute_trigger_before_java(
        ruletype in number,
        tempid in number,
        id in number,
        id2 in number default null,
        dateid in number default null,
        datetype in number default null)
        return varchar2 as

        retval varchar2(4000);
        retstr varchar2(4000);

        v_call_stack_count number;
        v_routine_type_id number;

        v_is_java number;
        v_user_id number;
        v_pid number;
        begin
            if disable_rules then
                return null;
            end if;

            v_is_java := pkg_sec.is_java;
            v_user_id := pkg_sec.get_cu;
            v_pid := pkg_sec.get_pid;

            retval := '';
            if tempid is null then
                for rec in (select r.rule_id,
                                   r.rule_class_id,
                                   r.is_async,
                                   upper(t.id_field) id_field
                            from rule r,
                                 rule_type t
                            where r.rule_type_id = t.rule_type_id
                                and r.rule_type_id = ruletype
                                and (select count(*) from rule_id_num n where n.rule_id = r.rule_id) = 0
                                and r.is_enabled = 1 and r.program_id = v_pid
                            order by r.order_number nulls last)
                loop
                    if rec.rule_class_id = 1 then
                        null;
                    --retstr := exec(rec.rule_id, rec.id_field, id, id2);
                    else
                        if v_is_java = 0 or rec.is_async = 1 then
                            insert into rule_queue (rule_id, user_id, id1, id2, rule_queue_status_id, created_ts, audit_log_id)
                            values (rec.rule_id, v_user_id, id, id2, 0, current_date, pkg_audit.get_last_audit_log_id);
                        elsif v_is_java = 1 and rec.is_async = 0 then
                            insert into rule_queue_temp(rule_id, user_id, id1, id2, audit_log_id)
                            values(rec.rule_id, v_user_id, id, id2, pkg_audit.get_last_audit_log_id);
                        end if;
                        retstr := '';
                    end if;

                    if retval is not null and retstr is not null then
                        retval := retval || chr(10) || retstr;
                    else
                        retval := retval || retstr;
                    end if;
                end loop;
            else
                for rec in (select r.rule_id,
                                   r.rule_class_id,
                                   r.is_async,
                                   upper(t.id_field) id_field
                            from rule r,
                                 rule_type t
                            where r.rule_type_id = t.rule_type_id
                                and r.rule_type_id = ruletype
                                and r.is_enabled = 1 and r.program_id = v_pid
                                and ((exists (select id_num from rule_id_num n where n.rule_id=r.rule_id and n.id_num = tempID)
                                          and (dateid is null or (dateid is not null and exists(select wp_task_date_type_id from rule_id_num n where n.rule_id=r.rule_id and n.wp_task_date_type_id = dateid)))
                                          and (datetype is null or (datetype is not null and exists(select is_start_wp_task_date from rule_id_num n where n.rule_id=r.rule_id and n.is_start_wp_task_date = datetype))))
                                         or (select count(id_num) from rule_id_num n where n.rule_id=r.rule_id ) = 0)
                            order by r.order_number nulls last)
                loop
                    if rec.rule_class_id = 1 then
                        null;
                    --retstr := exec(rec.rule_id, rec.id_field, id, tempid, id2);
                    else
                        if v_is_java = 0 or rec.is_async = 1 then
                            insert into rule_queue (rule_id, user_id, id1, id2, rule_queue_status_id, created_ts, audit_log_id)
                            values (rec.rule_id, v_user_id, id, id2, 0, current_date, pkg_audit.get_last_audit_log_id);
                        elsif v_is_java = 1 and rec.is_async = 0 then
                            insert into rule_queue_temp(rule_id, user_id, id1, id2, audit_log_id)
                            values(rec.rule_id, v_user_id, id, id2, pkg_audit.get_last_audit_log_id);
                        end if;
                        retstr := '';
                    end if;

                    if retval is not null and retstr is not null then
                        retval := retval || chr(10) || retstr;
                    else
                        retval := retval || retstr;
                    end if;
                end loop;
            end if;

            return retval;
        end execute_trigger_before_java;

    procedure execute_scheduled_rule_type(p_rule_type in rule.rule_type_id%type)
    as
        retstr varchar2(4000);
        v_rrid rule_run.rule_run_id%type;
        v_err_msg varchar2(4000);
        begin
            if not disable_rules then
                for rec in (select r.rule_id,
                                   upper(t.id_field) id_field
                            from rule r,
                                 rule_type t
                            where r.rule_type_id = t.rule_type_id
                                and r.rule_type_id = p_rule_type
                                and r.is_enabled = 1
                                and r.program_id = pkg_sec.get_pid
                                and not exists (select 1 from rule_id_num n where n.rule_id = r.rule_id)
                            order by r.order_number nulls last)
                loop
                    begin
                        v_rrid := create_rule_run(rec.rule_id, null);
                        retstr := exec(rec.rule_id, rec.id_field, null, null, null);
                        commit;
                        set_rule_run_status(v_rrid, c_executed_rule_status_id, current_date, retstr);
                        exception
                        when others then
                        v_err_msg := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
                        set_rule_run_status(v_rrid, c_failure_rule_status_id, current_date, null, v_err_msg);
                        rollback;
                    end;
                end loop;
            end if;
        end execute_scheduled_rule_type;

    function execute_trigger(
        ruletype in number,
        tempid in number,
        id in number,
        id2 in number default null,
        dateid in number default null,
        datetype in number default null)
        return varchar2 as

        retval varchar2(4000);
        retstr varchar2(4000);

        v_call_stack_count number;
        v_routine_type_id number;

        v_is_java number;
        v_user_id number;
        v_pid number;
        begin
            if disable_rules then
                return null;
            end if;

            v_is_java := pkg_sec.is_java;
            v_user_id := pkg_sec.get_cu;
            v_pid := pkg_sec.get_pid;

            retval := '';
            if tempid is null then
                for rec in (select r.rule_id,
                                   r.rule_class_id,
                                   r.is_async,
                                   upper(t.id_field) id_field
                            from rule r,
                                 rule_type t
                            where r.rule_type_id = t.rule_type_id
                                and r.rule_type_id = ruletype
                                and (select count(*) from rule_id_num n where n.rule_id = r.rule_id) = 0
                                and r.is_enabled = 1 and r.program_id = v_pid
                            order by r.order_number nulls last)
                loop
                    if rec.rule_class_id = 1 then
                        retstr := exec(rec.rule_id, rec.id_field, id, tempid, id2);
                    else
                        if v_is_java = 0 or rec.is_async = 1 then
                            if ruletype not in (20) then
                                insert into rule_queue (rule_id, user_id, id1, id2, rule_queue_status_id, created_ts, audit_log_id)
                                values (rec.rule_id, v_user_id, id, id2, 0, current_date, pkg_audit.get_last_audit_log_id);
                            end if;
                        elsif v_is_java = 1 and rec.is_async = 0 then
                            if ruletype not in (20) then
                                insert into rule_queue_temp(rule_id, user_id, id1, id2, audit_log_id)
                                values(rec.rule_id, v_user_id, id, id2, pkg_audit.get_last_audit_log_id);
                            end if;
                        end if;
                        retstr := '';
                    end if;

                    if retval is not null and retstr is not null then
                        retval := retval || chr(10) || retstr;
                    else
                        retval := retval || retstr;
                    end if;
                end loop;
            else
                for rec in (select r.rule_id,
                                   r.rule_class_id,
                                   r.is_async,
                                   upper(t.id_field) id_field
                            from rule r,
                                 rule_type t
                            where r.rule_type_id = t.rule_type_id
                                and r.rule_type_id = ruletype
                                and r.is_enabled = 1 and r.program_id = v_pid
                                and ((exists (select id_num from rule_id_num n where n.rule_id=r.rule_id and n.id_num = tempID)
                                          and (dateid is null or (dateid is not null and exists(select wp_task_date_type_id from rule_id_num n where n.rule_id=r.rule_id and n.wp_task_date_type_id = dateid)))
                                          and (datetype is null or (datetype is not null and exists(select is_start_wp_task_date from rule_id_num n where n.rule_id=r.rule_id and n.is_start_wp_task_date = datetype))))
                                         or (select count(id_num) from rule_id_num n where n.rule_id=r.rule_id ) = 0)
                            order by r.order_number nulls last)
                loop
                    if rec.rule_class_id = 1 then
                        retstr := exec(rec.rule_id, rec.id_field, id, tempid, id2);
                    else
                        if v_is_java = 0 or rec.is_async = 1 then
                            if ruletype not in (20) then
                                insert into rule_queue (rule_id, user_id, id1, id2, rule_queue_status_id, created_ts, audit_log_id)
                                values (rec.rule_id, v_user_id, id, id2, 0, current_date, pkg_audit.get_last_audit_log_id);
                            end if;
                        elsif v_is_java = 1 and rec.is_async = 0 then
                            if ruletype not in (20) then
                                insert into rule_queue_temp(rule_id, user_id, id1, id2, audit_log_id)
                                values(rec.rule_id, v_user_id, id, id2, pkg_audit.get_last_audit_log_id);
                            end if;
                        end if;
                        retstr := '';
                    end if;

                    if retval is not null and retstr is not null then
                        retval := retval || chr(10) || retstr;
                    else
                        retval := retval || retstr;
                    end if;
                end loop;
            end if;

            return retval;
        end execute_trigger;

    function execute_rule(ruleid number, id number)return varchar2 as
        retval varchar2(4000);
        retstr varchar2(4000);

        v_call_stack_count number;
        v_routine_type_id number;

        v_is_java number;
        v_user_id number;
        v_pid number;

        v_id_field rule_type.id_field%type;
        v_rule_class_id number;
        v_is_async number;
        begin
            if disable_rules then
                return null;
            end if;

            v_is_java := pkg_sec.is_java;
            v_user_id := pkg_sec.get_cu;
            v_pid := pkg_sec.get_pid;

            retval := '';

            begin
                select upper(t.id_field) id_field, r.rule_class_id, r.is_async into v_id_field, v_rule_class_id, v_is_async
                from rule r,
                     rule_type t
                where r.rule_type_id = t.rule_type_id
                    and r.rule_id = ruleid
                    and r.program_id = v_pid;
                exception
                when no_data_found then
                raise_application_error(-20000, 'Rule [' || ruleid || '] does''t exists');
            end;

            begin
                select upper(t.id_field) id_field, r.rule_class_id, r.is_async into v_id_field, v_rule_class_id, v_is_async
                from rule r,
                     rule_type t
                where r.rule_type_id = t.rule_type_id
                    and r.rule_id = ruleid
                    and r.is_enabled = 1
                    and r.program_id = v_pid;
                exception
                when no_data_found then
                return retval;
            end;

            if v_rule_class_id = 1 then
                retstr := exec(ruleid, v_id_field, id);
            else
                if v_is_java = 0 or v_is_async = 1 then
                    insert into rule_queue (rule_id, user_id, id1, id2, rule_queue_status_id, created_ts)
                    values (ruleid, v_user_id, id, null, 0, current_date);
                end if;
                retstr := '';
            end if;

            if retval is not null and retstr is not null then
                retval := retval || chr(10) || retstr;
            else
                retval := retval || retstr;
            end if;

            return retval;
        end execute_rule;

    procedure set_rule_run_status(
        p_rrid in rule_run.rule_run_id%type,
        p_status_id in process.status_id%type,
        p_end_date in process.end_date%type default null,
        p_return_str in rule_run.return_str%type default null,
        p_error_msg in process.error_message%type default null) as

        v_process_id process.process_id%type;

        pragma autonomous_transaction;
        begin
            select process_id into v_process_id from rule_run
            where rule_run_id = p_rrid;

            update process set status_id = p_status_id
            where process_id = v_process_id;

            if (p_end_date is not null) then
                update process
                   set end_date = p_end_date,
                       runtime = round((p_end_date-actual_start_date)*86400)
                 where process_id = v_process_id;
            end if;

            if (p_return_str is not null) then
                update rule_run set return_str = p_return_str
                where rule_run_id = p_rrid;
            end if;

            if (p_error_msg is not null) then
                update process set error_message = p_error_msg
                where process_id = v_process_id;
            end if;

            commit;
        end set_rule_run_status;

    function create_rule_run(
        ruleid number,
        ssql clob,
        p_id_num in rule_run.id_num%type default null,
        p_enable_trace in rule_run.enable_trace%type default 0)
        return number
    as
        v_rrid rule_run.rule_run_id%type;
        v_rr_proc_id process.process_id%type;

        pragma autonomous_transaction;
        begin

            insert into process (user_id, submission_date, start_date, actual_start_date, status_id, process_type_id, process_scheduler_id, program_id)
            values (pkg_sec.get_cu(), current_date, current_date, current_date, 1, 6, 1, pkg_sec.get_pid())
                returning process_id into v_rr_proc_id;

            insert into rule_run(process_id, rule_id, id_sql, rows_processed, id_num, enable_trace, program_id)
            values (v_rr_proc_id, ruleid, ssql, 0, p_id_num, p_enable_trace, pkg_sec.get_pid())
                returning rule_run_id into v_rrid;

            insert into rule_run_audit_call_stack
                (rule_run_id, order_number, routine_type_id, routine_id, routine2_id, routine_start_time, routine_text, program_id)
            select v_rrid, order_number, routine_type_id, routine_id, routine2_id, routine_start_time, routine_text, program_id
            from audit_call_stack_temp;

            commit;

            return v_rrid;
        end create_rule_run;

    procedure log_row(
        p_rrid in rule_run.rule_run_id%type,
        p_pk in rule_run_entity_pk.pk%type) as

        pragma autonomous_transaction;
        begin
            update rule_run set rows_processed = rows_processed + 1
            where rule_run_id = p_rrid;

            insert into rule_run_entity_pk(rule_run_id, pk)
            values(p_rrid, p_pk);

            commit;
        end log_row;

    function execute_rule_mass(p_rule_id in rule.rule_id%type, p_rule_run_id in rule_run.rule_run_id%type, p_ids tableofnum) return varchar2
    as
        retval varchar2(4000);
        v_err_msg varchar2(4000);
        v_i pls_integer;
    begin
        pkg_audit.call_stack_add_routine(7, p_rule_run_id, p_rule_id);

        retval := '';
        v_i := p_ids.first;
        while v_i is not null loop
            retval := retval || execute_rule(p_rule_id, p_ids(v_i));
            log_row(p_rule_run_id, p_ids(v_i));
            v_i := p_ids.next(v_i);
            commit;
        end loop;

        set_rule_run_status(p_rule_run_id, c_executed_rule_status_id, current_date, retval);
        pkg_audit.call_stack_del_routine(7, p_rule_run_id, p_rule_id);

        return retval;
    exception
        when others then
            v_err_msg := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
            set_rule_run_status(p_rule_run_id, c_failure_rule_status_id, current_date, retval, v_err_msg);
            pkg_audit.call_stack_del_routine(7, p_rule_run_id, p_rule_id);

        raise;
    end execute_rule_mass;

    procedure execute_mass_assignment(
        p_rule_run_id in rule_run.rule_run_id%type,
        p_cfid        in config_field.config_field_id%type,
        p_val         in clob,
        p_ids         in tableofnum)
    as

        c_date_cfdt constant number := 2;
        c_datetime_cfdt constant number := 90;
        c_time_cfdt constant number := 91;

        v_err_msg varchar2(4000);
        v_cfdt number;
        v_date_val date;
        v_date_fmt varchar2(50);
        v_retval varchar(4000);
        v_ttid config_field.xitor_type_id%type;
        v_i pls_integer;
    begin
        pkg_audit.call_stack_add_routine(7, p_rule_run_id, p_cfid);

        select data_type, xitor_type_id into v_cfdt, v_ttid
        from config_field where config_field_id = p_cfid;

        if (v_cfdt = c_date_cfdt) then
            v_date_fmt := pkg_user.get_cu_date_format;
        elsif (v_cfdt = c_datetime_cfdt) then
            v_date_fmt := pkg_user.get_cu_date_format || ' ' || pkg_user.get_cu_db_time_format;
        elsif (v_cfdt = c_time_cfdt) then
            v_date_fmt := pkg_user.get_cu_db_time_format;
        end if;

        v_i := p_ids.first;
        while v_i is not null loop
            if (v_cfdt = c_date_cfdt or v_cfdt = c_datetime_cfdt or v_cfdt = c_time_cfdt) then
                v_date_val := to_date(p_val, v_date_fmt);
                pkg_dl_support.set_cf_data_date(p_cfid, p_ids(v_i), v_date_val, 1, 1);
            else
                pkg_dl_support.set_cf_data(p_cfid, p_ids(v_i), p_val, 1, 1);
            end if;

            v_retval := execute_trigger(19, v_ttid, p_ids(v_i));

            log_row(p_rule_run_id, p_ids(v_i));
            v_i := p_ids.next(v_i);
            commit;
        end loop;

        set_rule_run_status(p_rule_run_id, c_executed_rule_status_id, current_date);
        pkg_audit.call_stack_del_routine(7, p_rule_run_id, p_cfid);
    exception
        when others then
            v_err_msg := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
            set_rule_run_status(p_rule_run_id, c_failure_rule_status_id, current_date, null, v_err_msg);
            pkg_audit.call_stack_del_routine(7, p_rule_run_id, p_cfid);

            raise;
    end execute_mass_assignment;

    procedure execute_mass_locks(
        p_rule_run_id in rule_run.rule_run_id%type,
        p_cfid in config_field.config_field_id%type,
        p_is_lock in number,
        p_ids in tableofnum) as

        v_err_msg varchar2(4000);
        v_pid config_field.program_id%type;
        v_i pls_integer;
    begin
        pkg_audit.call_stack_add_routine(c_form_btn_rule_rout_type_id, p_rule_run_id, p_cfid);

        select program_id into v_pid
        from config_field where config_field_id = p_cfid;

        v_i := p_ids.first;
        while v_i is not null loop
            if (p_is_lock = 1) then
                merge into config_field_lock l
                using (select p_ids(v_i) as key_value, p_cfid as config_field_id from dual) a
                on (a.key_value = l.key_value and a.config_field_id = l.config_field_id)
                when not matched then
                    insert (l.key_value, l.config_field_id, program_id)
                    values (a.key_value, a.config_field_id, v_pid);
            else
                delete from config_field_lock where config_field_id = p_cfid and key_value = p_ids(v_i);
            end if;

            log_row(p_rule_run_id, p_ids(v_i));
            v_i := p_ids.next(v_i);
            commit;
        end loop;

        set_rule_run_status(p_rule_run_id, c_executed_rule_status_id, current_date);
        pkg_audit.call_stack_del_routine(7, p_rule_run_id, p_cfid);
    exception
        when others then
            v_err_msg := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
            set_rule_run_status(p_rule_run_id, c_failure_rule_status_id, current_date, null, v_err_msg);
            pkg_audit.call_stack_del_routine(c_form_btn_rule_rout_type_id, p_rule_run_id, p_cfid);

            raise;
    end execute_mass_locks;

    function exec(
        p_rule_id in rule.rule_id%type,
        p_id_field in rule_type.id_field%type,
        p_id in number,
        p_id_num number default null,
        p_id2 in number default null)
        return varchar2 as

        v_sql clob;
        v_full_sql clob;
        v_retstr varchar2(32762);
        v_rule_name rule.rule%type;
        v_rule_type rule.rule_type_id%type;
        v_err_msg varchar2(500) := 'Exception in Rule "';
        v_rrid rule_run.rule_run_id%type;
        v_enable_trace rule.enable_trace%type;
        v_err_msg_rr varchar2(4000);

        begin
            select sql_text, rule, rule_type_id, enable_trace into v_sql, v_rule_name, v_rule_type, v_enable_trace
            from rule where rule_id=p_rule_id;

            v_sql := replace(v_sql, chr(13), ' ');

            if v_rule_type in (21,23) then
                v_sql := regexp_replace(v_sql, ':PARENT_ID', p_id, 1, 0, 'i');
                v_sql := regexp_replace(v_sql, ':CHILD_ID', p_id2, 1, 0, 'i');
            end if;

            if v_rule_type in (37, 38) then
                v_sql := regexp_replace(v_sql, ':IMP_RUN_ID', p_id, 1, 0, 'i');
            end if;

            if v_rule_type in (50, 51) then
                v_sql := regexp_replace(v_sql, ':PROCESS_ID', p_id, 1, 0, 'i');
            end if;

            if (v_rule_type = 45) then
                v_sql := regexp_replace(v_sql, ':USER_ID', p_id, 1, 0, 'i');
                v_sql := regexp_replace(v_sql, ':LOGIN_SUCCESSFUL', p_id2, 1, 0, 'i');
            end if;

            if (p_id_num is not null) then
                v_sql := regexp_replace(v_sql, ':ID_NUM', p_id_num, 1, 0, 'i');
            end if;

            v_full_sql := 'begin :RETURN_STR := ''''; ' || v_sql ||'end;';

            if ((p_id_field is not null) and
                (instr(upper(v_sql),':'||p_id_field)<>0)) then

                if (v_rule_type not in (4,35)) then
                    pkg_audit.call_stack_add_routine(1, p_rule_id);
                end if;
                begin
                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        v_rrid := create_rule_run(p_rule_id, null, p_id_num, v_enable_trace);
                    end if;

                    execute immediate v_full_sql using out v_retstr, in p_id;
                    pkg_audit.call_stack_del_routine(1, p_rule_id);

                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        log_row(v_rrid, p_id);
                        set_rule_run_status(v_rrid, c_executed_rule_status_id, current_date, v_retstr);
                    end if;
                    exception
                    when others then
                    pkg_audit.call_stack_del_routine(1, p_rule_id);

                    select rule into v_rule_name from rule where rule_id = p_rule_id;

                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        v_err_msg_rr := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
                        set_rule_run_status(v_rrid, c_failure_rule_status_id, current_date, null, v_err_msg_rr);
                    end if;

                    raise_application_error(-20000, v_err_msg || v_rule_name || '"', true);
                end;
            else
                if (v_rule_type not in (4,35)) then
                    pkg_audit.call_stack_add_routine(1, p_rule_id);
                end if;

                begin
                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        v_rrid := create_rule_run(p_rule_id, null, p_id_num, v_enable_trace);
                    end if;

                    execute immediate v_full_sql using out v_retstr;
                    pkg_audit.call_stack_del_routine(1, p_rule_id);

                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        log_row(v_rrid, p_id);
                        set_rule_run_status(v_rrid, c_executed_rule_status_id, current_date, v_retstr);
                    end if;
                    exception
                    when others then
                    pkg_audit.call_stack_del_routine(1, p_rule_id);

                    select rule into v_rule_name from rule where rule_id = p_rule_id;

                    if v_enable_trace = 1 and (v_rule_type not in (4,35,36,6,60)) then
                        v_err_msg_rr := sqlerrm || chr(13) || chr(10) || dbms_utility.format_error_backtrace;
                        set_rule_run_status(v_rrid, c_failure_rule_status_id, current_date, null, v_err_msg_rr);
                    end if;

                    raise_application_error(-20000, v_err_msg || v_rule_name || '"', true);
                end;
            end if;

            return v_retstr;
        end exec;

  function check_rule_id_nums(
        p_rule_id in rule.rule_id%type,
        p_rule_type_id rule_type.rule_type_id%type)
        return boolean
    as
        v_id_nums_count number;
        v_check_result boolean;
    begin
        v_check_result := true;
        if check_rule_requires_id_nums(p_rule_id, p_rule_type_id) then
            select count(*)
              into v_id_nums_count
              from rule_id_num rin
             where rin.rule_id = p_rule_id;
            if v_id_nums_count = 0 then
                v_check_result := false;
            end if;
        end if;
        return v_check_result;
    end check_rule_id_nums;

    function check_rule_requires_id_nums(
        p_rule_id in rule.rule_id%type,
        p_rule_type_id rule_type.rule_type_id%type)
        return boolean
    as
        v_is_id_nums_needed number;
    begin
        select count(*)
          into v_is_id_nums_needed
          from rule_type rt
         where p_rule_type_id != 4
           and rt.rule_type_id = p_rule_type_id
           and rt.template_sql is not null;
        return v_is_id_nums_needed > 0;
    end check_rule_requires_id_nums;
end pkg_ruleator;
/