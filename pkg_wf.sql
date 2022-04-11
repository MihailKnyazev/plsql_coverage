CREATE OR REPLACE PACKAGE BODY PKG_WF 
/*
 * Copyright 2003-2022 OneVizion, Inc. All rights reserved.
 */
as

    /**
     * Return true if p_field_spec is WP Task date's spec
     */
    function is_task_date(p_field_spec in varchar2) return boolean;

    /**
     * Return true if p_field_spec is Config Field of Date type
     */
    function is_date_cf(p_field_spec in varchar2) return boolean;

    /**
     * Creates new WF from the template and returns wf_workflow_id.
     * start_new_process_int must be called to complete WF startup
     */
    function create_wf(
        p_proc_id in wf_template.wf_template_id%type,
        p_workflow_name in wf_workflow.workflow_name%type,
        p_owner_uid in wf_workflow.owner_user_id%type,
        p_workflow_description wf_workflow.description%type default null)
        return wf_workflow.wf_workflow_id%type;

    /**
     * Copies steps from template and starts WF
     */
    function start_new_process_int(
        p_wf_id wf_workflow.wf_workflow_id%type,
        p_assignee_uid in wf_workflow.owner_user_id%type)
        return list_id;

    /**
     * Search next_wf_step_id for decision option based on wf_workflow_id,
     * wf_template_decision and option's order_number
     */
    function get_decision_next_step_id(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_dec_id in wf_template_decision.wf_template_decision_id%type,
        p_order_num in wf_template_decision_option.order_number%type)
        return wf_decision_option.next_wf_step_id%type;

    /**
     * Return next_wf_step_id for specified step run
     */
    function get_next_step_id(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_cur_step_proc_id in wf_step.wf_template_step_id%type)
        return wf_step.next_wf_step_id%type;

    procedure exec_plsql_block(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_run_step_id wf_step.wf_step_id%type,
        p_plsql in wf_step.plsql_block%type);


    procedure exec_decision(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_step_id in wf_step.wf_step_id%type,
        p_decision_id in wf_decision.wf_decision_id%type);

    function start_new_process_int(
        p_wf_id wf_workflow.wf_workflow_id%type,
        p_assignee_uid in wf_workflow.owner_user_id%type)
        return list_id
    as
        v_wf_template_id wf_workflow.wf_template_id%type;
        v_step_id wf_step.wf_step_id%type;
        v_des_id wf_decision.wf_decision_id%type;
        v_next_step_id wf_step.next_wf_step_id%type;
        v_first_wf_step_id wf_workflow.first_wf_step_id%type;
        v_is_def_users integer;
        v_rule_retval varchar2(4000);

        cursor cur_steps(p_wfproc_id in wf_template.wf_template_id%type) is
            select s.wf_template_step_id,
                   s.step_type_id,
                   s.title_label_id,
                   s.step_name,
                   s.description step_desc,
                   s.plsql_block,
                   s.key_value_field_id,
                   s.next_wf_template_step_id,
                   s.is_can_return,
                   s.is_loop,
                   d.wf_template_decision_id,
                   d.description des_desc,
                   d.is_inclusive_decision,
                   d.decision_name,
                   s.block_pos_x,
                   s.block_pos_y,
                   s.disable_navigation,
                   s.status_config_field_id,
                   s.go_to_next_title_label_id,
                   s.go_to_previous_title_label_id,
                   s.next_step_confirm_label_id,
                   s.prev_step_confirm_label_id
              from wf_template_step s
              left join wf_template_decision d on d.wf_template_decision_id = s.wf_template_decision_id
             where s.wf_template_id = p_wfproc_id;
    begin
        select wf_template_id
           into v_wf_template_id
          from wf_workflow
         where wf_workflow_id = p_wf_id;


        --Copy steps and decisions
        for rec in cur_steps(v_wf_template_id) loop
            select decode(count(1),0,0,1)
              into v_is_def_users
              from wf_template_step_user_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_step (
                wf_workflow_id,
                wf_template_step_id,
                step_type_id,
                title_label_id,
                description,
                plsql_block,
                is_can_return,
                key_value_field_id,
                step_name,
                is_default_user,
                is_loop,
                block_pos_x,
                block_pos_y,
                disable_navigation,
                status_config_field_id,
                go_to_next_title_label_id,
                go_to_previous_title_label_id,
                next_step_confirm_label_id,
                prev_step_confirm_label_id
            ) values (
                p_wf_id,
                rec.wf_template_step_id,
                rec.step_type_id,
                rec.title_label_id,
                rec.step_desc,
                rec.plsql_block,
                rec.is_can_return,
                rec.key_value_field_id,
                rec.step_name,
                v_is_def_users,
                rec.is_loop,
                rec.block_pos_x,
                rec.block_pos_y,
                rec.disable_navigation,
                rec.status_config_field_id,
                rec.go_to_next_title_label_id,
                rec.go_to_previous_title_label_id,
                rec.next_step_confirm_label_id,
                rec.prev_step_confirm_label_id
            ) returning wf_step_id
                   into v_step_id;

            if v_is_def_users = 0 and p_assignee_uid is not null then
                insert into wf_step_user_xref(wf_step_id, user_id)
                values (v_step_id, p_assignee_uid);
            else
                insert into wf_step_user_xref(wf_step_id, user_id)
                select v_step_id,
                       user_id
                  from wf_template_step_user_xref
                 where wf_template_step_id = rec.wf_template_step_id;
            end if;

            insert into wf_step_role_xref(wf_step_id, sec_role_id)
            select v_step_id,
                   sec_role_id
              from wf_template_step_role_xref
             where wf_template_step_id = rec.wf_template_step_id;

            if (rec.wf_template_decision_id is not null) then
                insert into wf_decision (
                    description,
                    is_inclusive_decision,
                    decision_name,
                    wf_template_decision_id
                ) values (
                    rec.des_desc,
                    rec.is_inclusive_decision,
                    rec.decision_name,
                    rec.wf_template_decision_id
                ) returning wf_decision_id
                       into v_des_id;

                update wf_step
                   set wf_decision_id = v_des_id
                 where wf_step_id = v_step_id;

                insert into wf_decision_option (
                    order_number,
                    expression,
                    is_default,
                    option_name,
                    wf_decision_id,
                    goto_end,
                    wf_template_decision_opt_id
                ) select order_number,
                         expression,
                         is_default,
                         option_name,
                         v_des_id,
                         goto_end,
                         wf_template_decision_opt_id
                    from wf_template_decision_option
                   where wf_template_decision_id = rec.wf_template_decision_id;
            end if;


            --Copy wf_step_*_XREF tables
            insert into wf_step_capp_xref (wf_step_id, config_app_id)
            select v_step_id,
                   config_app_id
              from wf_template_step_capp_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_step_cgroup_xref(wf_step_id, config_group_id, order_number)
            select v_step_id,
                   config_group_id,
                   order_number
              from wf_template_step_cgroup_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_step_cfield_xref(
                wf_step_id,
                config_field_id,
                order_number,
                field_name,
                template_task_id
            ) select v_step_id,
                     config_field_id,
                     order_number,
                     field_name,
                     template_task_id
                from wf_template_step_cfield_xref
               where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_step_ttype_xref (wf_step_id, trackor_type_id)
            select v_step_id,
                   trackor_type_id
              from wf_template_step_ttype_xref
             where wf_template_step_id = rec.wf_template_step_id;
        end loop;

        for rec in cur_steps(v_wf_template_id) loop
            --Fill wf_step.next_wf_step_id
            if (rec.next_wf_template_step_id is not null) then
                v_next_step_id := get_next_step_id(p_wf_id, rec.wf_template_step_id);

                update wf_step
                   set next_wf_step_id = v_next_step_id
                 where wf_template_step_id = rec.wf_template_step_id
                   and wf_workflow_id = p_wf_id;
            end if;

            --Fill wf_decision_option.next_wf_step_id
            if (rec.wf_template_decision_id is not null) then
                select rd.wf_decision_id
                  into v_des_id
                  from wf_decision rd join wf_step rstep on (rstep.wf_decision_id = rd.wf_decision_id)
                 where rd.wf_template_decision_id = rec.wf_template_decision_id
                   and rstep.wf_workflow_id = p_wf_id;

                for rec_dec_opt in (
                    select order_number
                      from wf_template_decision_option
                     where wf_template_decision_id = rec.wf_template_decision_id)
                loop
                    v_next_step_id := get_decision_next_step_id(p_wf_id, rec.wf_template_decision_id, rec_dec_opt.order_number);

                    update wf_decision_option
                       set next_wf_step_id = v_next_step_id
                     where order_number = rec_dec_opt.order_number
                       and wf_decision_id = v_des_id;
                end loop;
            end if;
        end loop;

        --Fill first step
        select rs.wf_step_id
          into v_first_wf_step_id
          from wf_step rs join wf_template proc on (proc.first_template_step_id = rs.wf_template_step_id)
         where rs.wf_workflow_id = p_wf_id;

        update wf_workflow
           set first_wf_step_id = v_first_wf_step_id
         where wf_workflow_id = p_wf_id;

        v_rule_retval := pkg_ruleator.execute_trigger(41, v_wf_template_id, p_wf_id);

        return(next_step(p_wf_id, null));
    end start_new_process_int;


    function create_wf(
        p_proc_id in wf_template.wf_template_id%type,
        p_workflow_name in wf_workflow.workflow_name%type,
        p_owner_uid in wf_workflow.owner_user_id%type,
        p_workflow_description wf_workflow.description%type default null)
        return wf_workflow.wf_workflow_id%type
    as
        v_wf_id wf_workflow.wf_workflow_id%type;
    begin
        v_wf_id := seq_wf_workflow_id.nextval;

        insert into wf_workflow(
            wf_workflow_id,
            wf_template_id,
            wf_state_id,
            workflow_name,
            owner_user_id,
            start_date,
            description,
            key_value_field_id,
            is_mobile,
            config_field_coord_link_id,
            end_block_pos_x,
            end_block_pos_y,
            sort_field_id
        ) select v_wf_id,
                 p_proc_id,
                 c_state_running,
                 p_workflow_name,
                 p_owner_uid,
                 current_date,
                 p_workflow_description,
                 key_value_field_id,
                 is_mobile,
                 config_field_coord_link_id,
                 end_block_pos_x,
                 end_block_pos_y,
                 sort_field_id
            from wf_template
           where wf_template_id = p_proc_id;

        return v_wf_id;
    end create_wf;


    function start_new_process(
        p_proc_id in wf_template.wf_template_id%type,
        p_workflow_name in wf_workflow.workflow_name%type,
        p_owner_uid in wf_workflow.owner_user_id%type,
        p_assign_owner in number default 0,
        p_workflow_description in wf_workflow.description%type default null)
        return list_id
    as
        v_wf_id wf_workflow.wf_workflow_id%type;
        v_assignee_uid users.user_id%type;

    begin
        v_wf_id := create_wf(p_proc_id, p_workflow_name, p_owner_uid, p_workflow_description);

        --If user doesn't have admin permission on WF assign in to all proc steps
        --except steps with default user assigned
        if (p_assign_owner = 1) then
            v_assignee_uid := p_owner_uid;
        end if;

        return start_new_process_int(v_wf_id, v_assignee_uid);
    end start_new_process;


    function start_new_process_with_key_val(
        p_wf_template_id in wf_template.wf_template_id%type,
        p_owner_uid in wf_workflow.owner_user_id%type,
        p_key_val in wf_step.key_value%type,
        p_workflow_name in wf_workflow.workflow_name%type  default null,
        p_workflow_description in wf_workflow.description%type default null)
        return list_id
    is
        v_workflow_name wf_workflow.workflow_name%type;
        v_wf_id wf_workflow.wf_workflow_id%type;
        v_key_val_cfid wf_workflow.key_value_field_id%type;

    begin
        if p_workflow_name is null then
            select template_name into v_workflow_name
              from wf_template
             where wf_template_id = p_wf_template_id;
        else
            v_workflow_name := p_workflow_name;
        end if;

        v_wf_id := create_wf(p_wf_template_id, v_workflow_name, p_owner_uid, p_workflow_description);

        select key_value_field_id into v_key_val_cfid
          from wf_workflow
         where wf_workflow_id = v_wf_id;

        if (v_key_val_cfid is not null) then
            pkg_dl_support.set_cf_data_num(v_key_val_cfid, v_wf_id, p_key_val, 1, 1);
        end if;

        return start_new_process_int(v_wf_id, p_owner_uid);
    end start_new_process_with_key_val;


    function first_next_step(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_finished_step in wf_step.wf_step_id%type)
        return wf_step.wf_step_id%type
    as
        v_next_steps list_id;
        v_ret wf_step.wf_step_id%type;
    begin
        v_next_steps := next_step(p_run_id, p_finished_step);
        if not (v_next_steps is null or v_next_steps.count = 0) then
            v_ret := v_next_steps(1);
        end if;

        return v_ret;
    end first_next_step;

    function next_step(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_finished_step in wf_step.wf_step_id%type)
        return list_id
    as
        v_next_step_id wf_step.wf_step_id%type;
        v_decision_id wf_step.wf_decision_id%type;
        v_next_steps list_id;
        v_next_steps2 list_id;
        rt_rstep wf_step%rowtype;
        v_finish_date wf_step.finish_date%type;
        v_key_val number;
        v_next_steps_exist boolean;
        v_finished_step_iteration wf_step.iteration%type;
        v_started_step_iteration wf_step.iteration%type;
        v_wf_template_step_id wf_template_step.wf_template_step_id%type;
        v_i pls_integer;
        v_next_steps_count number;
        v_cu users.user_id%type := pkg_sec.get_cu;
    begin
        if (p_finished_step is null) then
            select first_wf_step_id into v_next_step_id
              from wf_workflow where wf_workflow_id = p_run_id;

            v_next_steps2 := list_id(v_next_step_id);

        else
            delete from wf_next_step
             where wf_workflow_id = p_run_id and next_wf_step_id = p_finished_step;

            update wf_step set finish_date = current_date
             where wf_step_id = p_finished_step
                 returning next_wf_step_id, wf_decision_id, iteration, wf_template_step_id
                     into v_next_step_id, v_decision_id, v_finished_step_iteration, v_wf_template_step_id;

            begin
                insert into audit_log_wf_step(wf_step_id, wf_template_step_id, iteration, user_id, is_start, ts)
                values (p_finished_step, v_wf_template_step_id, v_finished_step_iteration, v_cu, 0, current_date);
            exception
                when dup_val_on_index then
                    --log entry my exists because user presse "Prev Step" button, in this case just update date
                    update audit_log_wf_step set ts = current_date
                     where wf_step_id = p_finished_step and iteration = v_finished_step_iteration and is_start = 0;
            end;

            if (v_decision_id is null) and (v_next_step_id is not null) then
                v_next_steps2 := list_id(v_next_step_id);
            elsif (v_decision_id is null) then -- last step
                v_next_steps2 := list_id();
            else
                select next_wf_step_id bulk collect into v_next_steps2
                  from wf_decision_option
                 where wf_decision_id = v_decision_id and is_true = 1
                         and next_wf_step_id is not null;
            end if;

            if (v_next_steps2.count = 0) then --wf thread complete
                -- check if there are other threads still running
                select next_wf_step_id bulk collect into v_next_steps2
                  from wf_next_step
                 where wf_workflow_id = p_run_id;

                if (v_next_steps2.count = 0) then --no other running threads - wf complete
                    update wf_workflow set wf_state_id = 4, finish_date = current_date
                     where wf_workflow_id = p_run_id;
                    return null;

                else
                    return v_next_steps2; -- there are other threads running
                end if;
            end if;

            v_i := v_next_steps2.first;
            while v_i is not null loop
                select finish_date into v_finish_date
                  from wf_step where wf_step_id = v_next_steps2(v_i);

                if (v_finish_date is not null) then --new iteration
                    update wf_step set iteration = iteration + 1,
                        start_date = null, finish_date = null,
                        start_notif_processed = 0, finish_notif_processed = 0
                     where wf_step_id = v_next_steps2(v_i);
                end if;

                v_i := v_next_steps2.next(v_i);
            end loop;
        end if;

        v_next_steps := list_id();
        v_i := v_next_steps2.first;

        while v_i is not null loop
            select * into rt_rstep from wf_step where wf_step_id = v_next_steps2(v_i);

            if (rt_rstep.key_value_field_id is null) then
                v_key_val := p_run_id;
            else
                v_key_val := pkg_cfrpt.getValNumNLByID(p_run_id, rt_rstep.key_value_field_id);
            end if;

            update wf_step set key_value = v_key_val, start_date = current_date
             where wf_step_id = v_next_steps2(v_i);

            if (is_interactive_step(rt_rstep.step_type_id)) then
                v_next_steps := v_next_steps multiset union distinct list_id(v_next_steps2(v_i));

                select count(1) into v_next_steps_count 
                  from wf_next_step
                 where wf_workflow_id = p_run_id
                   and next_wf_step_id = v_next_steps2(v_i);

                if v_next_steps_count = 0 then
                    insert into wf_next_step(wf_workflow_id, wf_step_id, next_wf_step_id)
                    values (p_run_id, p_finished_step, v_next_steps2(v_i));
                end if;

            elsif (rt_rstep.step_type_id = 9) then --PL/SQL Block
                exec_plsql_block(p_run_id, rt_rstep.wf_step_id, rt_rstep.plsql_block);
                v_next_steps := v_next_steps multiset union distinct next_step(p_run_id, rt_rstep.wf_step_id);

            elsif (rt_rstep.wf_decision_id is not null) then --Decision
                exec_decision(p_run_id, rt_rstep.wf_step_id, rt_rstep.wf_decision_id);
                v_next_steps := v_next_steps multiset union distinct next_step(p_run_id, rt_rstep.wf_step_id);
            end if;

            v_i := v_next_steps2.next(v_i);
        end loop;

        v_next_steps_exist := (v_next_steps is not null) and (v_next_steps.count > 0);
        if (v_next_steps_exist) then
            v_i := v_next_steps.first;
            while v_i is not null loop
                select iteration, wf_template_step_id into v_started_step_iteration, v_wf_template_step_id
                  from wf_step where wf_step_id = v_next_steps(v_i);

                begin
                    insert into audit_log_wf_step(wf_step_id, wf_template_step_id, iteration, user_id, is_start, ts)
                    values (v_next_steps(v_i), v_wf_template_step_id, v_started_step_iteration, v_cu, 1, current_date);
                exception
                    when dup_val_on_index then
                        null;
                end;

                v_i := v_next_steps.next(v_i);
            end loop;
        end if;

        return v_next_steps;
    end next_step;


    function evaluate_expression(
        p_expr in wf_template_decision_option.expression%type,
        p_step_id in wf_step.wf_step_id%type,
        p_key_value in number,
        p_subkey_value in number default null,
        p_wpkey_value in number default null,
        p_run_id in wf_workflow.wf_workflow_id%type)
        return number
    as
        v_expr wf_template_decision_option.expression%type;
        v_pos1 integer;
        v_pos2 integer;
        v_field_spec varchar2(300);
        v_key_xtid xitor.xitor_type_id%type;
        v_subkey_xtid xitor.xitor_type_id%type;
        v_val varchar2(100);
        v_cur binary_integer;
        v_ignore_me integer;
        v_result_num number(1);
        i integer;

        type t_bind_vals is table of varchar2(100);
        v_bind_vals t_bind_vals;

        type t_is_date_bind_val is table of boolean;
        v_is_date_bind_val t_is_date_bind_val;
    begin
        begin
            select xitor_type_id into v_key_xtid from xitor where xitor_id = p_key_value;
        exception
            when no_data_found then
                null;
        end;

        begin
            select xitor_type_id into v_subkey_xtid from xitor where xitor_id = p_subkey_value;
        exception
            when no_data_found then
                null;
        end;

        --remove leading and trailing line breaks
        v_expr := replace(p_expr, chr(10), ' ');
        v_expr := replace(v_expr, chr(13), ' ');
        v_expr := trim(both chr(10) from v_expr);
        v_expr := trim(both chr(13) from v_expr);
        v_expr := trim(both chr(10) from v_expr);

        v_bind_vals := t_bind_vals();
        v_is_date_bind_val := t_is_date_bind_val();
        loop
            v_pos1 := instr(v_expr, '[');

            exit when v_pos1 = 0;

            v_pos2 := instr(v_expr, ']');
            v_field_spec := substr(v_expr, v_pos1 + 1, v_pos2 - v_pos1 - 1);

            if (upper(v_field_spec) = 'WF_STEP_ID') then
                v_val := p_step_id;
            elsif (upper(v_field_spec) = 'WF_WORKFLOW_ID') then
                v_val := p_run_id;
            else
                v_val := get_expression_val(v_field_spec, p_key_value, v_key_xtid,
                                            p_subkey_value, v_subkey_xtid, p_wpkey_value, p_run_id);
            end if;

            v_bind_vals.extend;
            v_bind_vals(v_bind_vals.count()) := v_val;
            v_expr := replace(v_expr, '[' || v_field_spec || ']', ':p' || v_bind_vals.count());

            v_is_date_bind_val.extend;
            if is_task_date(v_field_spec) or is_date_cf(v_field_spec) then
                v_is_date_bind_val(v_is_date_bind_val.count()) := true;
            else
                v_is_date_bind_val(v_is_date_bind_val.count()) := false;
            end if;
        end loop;

        /*dbms_sql can't return boolean values, I'm using number*/
        v_expr := 'declare v_result_bool boolean; ' ||
                  'begin v_result_bool := ' || v_expr || '; ' ||
                  'if v_result_bool then :p_result_num := 1; else :p_result_num :=0; end if; end;';

        v_cur := dbms_sql.open_cursor;
        dbms_sql.parse(v_cur, v_expr, dbms_sql.native);

        i := v_bind_vals.first;
        while i is not null loop
            if (v_is_date_bind_val(i)) then
                dbms_sql.bind_variable(v_cur, ':p' || i, to_date(v_bind_vals(i), 'MM/DD/YYYY'));
            else
                dbms_sql.bind_variable(v_cur, ':p' || i, v_bind_vals(i));
            end if;

            i := v_bind_vals.next(i);
        end loop;

        dbms_sql.bind_variable(v_cur, ':p_result_num', 1);
        v_ignore_me := dbms_sql.execute(v_cur);
        dbms_sql.variable_value(v_cur, ':p_result_num', v_result_num);
        dbms_sql.close_cursor(v_cur);

        return v_result_num;
    exception
        when others then
            if dbms_sql.is_open(v_cur) then
                dbms_sql.close_cursor(v_cur);
            end if;
            raise;
    end evaluate_expression;


    function get_expression_val(
        p_field_spec in varchar2,
        p_key_value in number,
        p_key_xtid in xitor.xitor_type_id%type,
        p_subkey_value in number,
        p_subkey_xtid in xitor.xitor_type_id%type,
        p_wpkey_value in number,
        p_run_id in wf_workflow.wf_workflow_id%type)
        return varchar2
    as
        v_xtid xitor_type.xitor_type_id%type;
        v_cfid config_field.config_field_id%type;
        v_xid number;
        v_val varchar2(100);
        v_xt xitor_type.xitor_type%type;
        v_cfname config_field.config_field_name%type;
        v_pos1 integer;
        v_date_type varchar2(100);
        v_sf_date varchar2(7);
        v_data_type config_field.data_type%type;
        v_pid number;
    begin
        if (upper(p_field_spec) = 'KEY_VALUE') then
            return p_key_value;
        elsif (upper(p_field_spec) = 'SUBKEY_VALUE') then
            return p_subkey_value;
        elsif (upper(p_field_spec) = 'WPKEY_VALUE') then
            return p_wpkey_value;
        end if;

        v_pos1 := instr(p_field_spec, '.');
        v_xt := substr(p_field_spec, 1, v_pos1 - 1);
        v_cfname := substr(p_field_spec, v_pos1 + 1, length(p_field_spec) - v_pos1);

        if is_task_date(p_field_spec) then
            if (upper(substr(v_xt, length(v_xt) - 5, 6)) = '_START') then
                v_date_type := substr(v_xt, 1, length(v_xt) - 6);
                v_sf_date := 'S';
            else
                v_date_type := substr(v_xt, 1, length(v_xt) - 7);
                v_sf_date := 'F';
            end if;

            v_val := pkg_rpt.ord_date(p_wpkey_value, p_key_xtid,
                                      to_number(v_cfname), v_date_type, v_sf_date);
        else
            select program_id into v_pid from wf_workflow where wf_workflow_id = p_run_id;

            select xitor_type_id into v_xtid from xitor_type
             where upper(xitor_type) = upper(v_xt) and
                 (program_id = v_pid or program_id is null);

            if (v_xtid = p_key_xtid) then
                v_xid := p_key_value;
            elsif (v_xtid = p_subkey_xtid) then
                v_xid := p_subkey_value;
            elsif (v_xtid = 99) then
                v_xid := p_wpkey_value;
            elsif(v_xtid = 10) then
                v_xid := p_run_id;
            else
                raise_application_error(-20000, pkg_label.format(17751, pkg_label.list_label_params('field_spec' => p_field_spec)));
            end if;

            select config_field_id, data_type
              into v_cfid, v_data_type
              from config_field
             where upper(config_field_name) = upper(v_cfname)
               and xitor_type_id = v_xtid
               and program_id = v_pid
               and (is_static = 0 or config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id));

            if (v_data_type = 2) then
                v_val := to_char(pkg_config_field_rpt.getValDateById(v_xid, v_cfid), 'MM/DD/YYYY');
            else
                v_val := pkg_config_field_rpt.getValStrById(v_xid, v_cfid);
            end if;

        end if;

        return v_val;
    exception
        when no_data_found then
            raise_application_error(-20000, pkg_label.format(17752, pkg_label.list_label_params('field_spec' => p_field_spec)));
    end get_expression_val;


    function is_task_date(p_field_spec in varchar2) return boolean as
        v_pos1 integer;
        v_xt xitor_type.xitor_type%type;
        v_cfname config_field.config_field_name%type;
    begin
        v_pos1 := instr(p_field_spec, '.');
        v_xt := substr(p_field_spec, 1, v_pos1 - 1);
        v_cfname := substr(p_field_spec, v_pos1 + 1, length(p_field_spec) - v_pos1);
        return pkg_str.is_number(v_cfname) and
               ((upper(substr(v_xt, length(v_xt) - 5, 6)) = '_START') or
                (upper(substr(v_xt, length(v_xt) - 6, 7)) = '_FINISH'));
    end is_task_date;


    function is_date_cf(p_field_spec in varchar2) return boolean as
        v_pos1 integer;
        v_xt xitor_type.xitor_type%type;
        v_cfname config_field.config_field_name%type;
        v_data_type config_field.data_type%type;
        v_ret boolean;
        v_pid program.program_id%type := pkg_sec.get_pid;
    begin
        v_pos1 := instr(p_field_spec, '.');
        v_xt := substr(p_field_spec, 1, v_pos1 - 1);
        v_cfname := substr(p_field_spec, v_pos1 + 1, length(p_field_spec) - v_pos1);

        select --+index(cf, idx12_config_field)
               cf.data_type
          into v_data_type
          from config_field cf
          join xitor_type xt on (xt.xitor_type_id = cf.xitor_type_id)
         where upper(cf.config_field_name) = upper(v_cfname)
           and upper(xt.xitor_type) = upper(v_xt)
           and cf.program_id = v_pid
           and (cf.is_static = 0 or cf.config_field_name in (pkg_cf.c_static_xitor_key, pkg_cf.c_static_xitor_class_id));

        if (v_data_type = 2) then
            v_ret := true;
        else
            v_ret := false;
        end if;

        return v_ret;
    exception
        when no_data_found then
            return false;
    end is_date_cf;


    function is_interactive_step(p_step_type_id in wf_step.step_type_id%type)
        return boolean
    as
        v_ret boolean;
    begin
        if (p_step_type_id not in(8, 9)) then
            v_ret := true;
        else
            v_ret := false;
        end if;

        return v_ret;
    end is_interactive_step;


    function get_decision_next_step_id(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_dec_id in wf_template_decision.wf_template_decision_id%type,
        p_order_num in wf_template_decision_option.order_number%type)
        return wf_decision_option.next_wf_step_id%type
    as
        v_next_step_id wf_decision_option.next_wf_step_id%type;
    begin
        select ns.wf_step_id into v_next_step_id
          from wf_step s
          join wf_decision d on s.wf_decision_id = d.wf_decision_id
          join wf_decision_option do on d.wf_decision_id = do.wf_decision_id
          join wf_template_decision_option tdo on do.wf_template_decision_opt_id = tdo.wf_template_decision_opt_id
          join wf_template_step ts on tdo.next_wf_template_step_id = ts.wf_template_step_id
          join wf_step ns on ts.wf_template_step_id = ns.wf_template_step_id
         where  s.wf_workflow_id = p_run_id
                 and ns.wf_workflow_id = p_run_id
                 and d.wf_template_decision_id = p_dec_id
                 and do.order_number = p_order_num;

        return v_next_step_id;
    exception
        when no_data_found then
            return null;
    end get_decision_next_step_id;


    function get_next_step_id(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_cur_step_proc_id in wf_step.wf_template_step_id%type)
        return wf_step.next_wf_step_id%type
    as
        v_next_run_step_id wf_step.next_wf_step_id%type;
    begin
        select rs_next.wf_step_id into v_next_run_step_id
          from wf_step rs join wf_template_step s
                               on (s.wf_template_step_id = rs.wf_template_step_id)
          join wf_step rs_next
               on (rs_next.wf_template_step_id = s.next_wf_template_step_id)
         where rs.wf_workflow_id = p_run_id and rs_next.wf_workflow_id = p_run_id
                 and s.wf_template_step_id = p_cur_step_proc_id;

        return v_next_run_step_id;
    end get_next_step_id;

    procedure exec_plsql_block(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_run_step_id wf_step.wf_step_id%type,
        p_plsql in wf_step.plsql_block%type)
    as
        v_cur binary_integer;
        v_ignore_me integer;
        v_bind_val number;
        v_plsql wf_step.plsql_block%type;
        v_plsql_lcase wf_step.plsql_block%type;
        v_key_cfid config_field.config_field_id%type;
        v_subkey_cfid config_field.config_field_id%type;
        v_wpkey_cfid config_field.config_field_id%type;

        e_bind_var_not_exists exception;
        pragma exception_init (e_bind_var_not_exists, -01006);

        e_custom_expected_error exception;
        pragma exception_init (e_custom_expected_error, -20000);
    begin
        if (trim(' ' from p_plsql) is null) then -- skip empty block
            return;
        end if;

        select key_value_field_id into v_key_cfid
          from wf_step where wf_step_id = p_run_step_id;

        v_plsql := replace(p_plsql, chr(13), ' ');
        v_plsql_lcase := lower(v_plsql);

        v_cur := dbms_sql.open_cursor;
        dbms_sql.parse(v_cur, v_plsql, dbms_sql.native);

        if (instr(v_plsql_lcase, ':key') <> 0) then
            v_bind_val := pkg_cfrpt.getValNumNLByID(p_run_id, v_key_cfid);
            begin
                dbms_sql.bind_variable(v_cur, ':key', v_bind_val);
            exception
                when e_bind_var_not_exists then
--PL/SQL line with var may be commented
                    null;
            end;
        end if;

        if (instr(v_plsql_lcase, ':subkey') <> 0) then
            v_bind_val := pkg_cfrpt.getValNumNLByID(p_run_id, v_subkey_cfid);
            begin
                dbms_sql.bind_variable(v_cur, ':subkey', v_bind_val);
            exception
                when e_bind_var_not_exists then
--PL/SQL line with var may be commented
                    null;
            end;
        end if;

        if (instr(v_plsql_lcase, ':wpkey') <> 0) then
            v_bind_val := pkg_cfrpt.getValNumNLByID(p_run_id, v_wpkey_cfid);
            begin
                dbms_sql.bind_variable(v_cur, ':wpkey', v_bind_val);
            exception
                when e_bind_var_not_exists then
--PL/SQL line with var may be commented
                    null;
            end;
        end if;

        if (instr(v_plsql_lcase, ':wf_workflow_id') <> 0) then
            begin
                dbms_sql.bind_variable(v_cur, ':wf_workflow_id', p_run_id);
            exception
                when e_bind_var_not_exists then
--PL/SQL line with var may be commented
                    null;
            end;
        end if;

        pkg_audit.call_stack_add_routine(14, p_run_step_id, p_run_id);

        v_ignore_me := dbms_sql.execute(v_cur);
        dbms_sql.close_cursor(v_cur);

        pkg_audit.call_stack_del_routine(14, p_run_step_id, p_run_id);
    exception
        when e_custom_expected_error then
            if dbms_sql.is_open(v_cur) then
                dbms_sql.close_cursor(v_cur);
            end if;
            raise;
        when others then
            if dbms_sql.is_open(v_cur) then
                dbms_sql.close_cursor(v_cur);
            end if;
            raise_application_error(pkg_err_code.c_errcode_wf_plsql_block_execution,
                                    pkg_label.format_wrapped(6313, pkg_label.list_label_params('wfStepId' => p_run_step_id)),
                                    true);
    end exec_plsql_block;


    procedure exec_decision(
        p_run_id in wf_workflow.wf_workflow_id%type,
        p_step_id in wf_step.wf_step_id%type,
        p_decision_id in wf_decision.wf_decision_id%type)
        is
        v_key_val number;
        v_subkey_val number;
        v_wpkey_val number;
        v_is_inclusive wf_decision.is_inclusive_decision%type;
        v_expr_val_num number(1);
        v_is_dec_found boolean := false;
    begin
        select key_value, subkey_value, wpkey_value
          into v_key_val, v_subkey_val, v_wpkey_val
          from wf_step where wf_step_id = p_step_id;

        select is_inclusive_decision into v_is_inclusive
          from wf_decision where wf_decision_id = p_decision_id;

--need to reset evaluation results in case of iterations
        update wf_decision_option set is_true = 0
         where wf_decision_id = p_decision_id;

        for rec in (select is_default, expression, wf_decision_id, order_number
                      from wf_decision_option
                     where wf_decision_id = p_decision_id
                     order by is_default, order_number) loop

            if (rec.is_default <> 1) then
                v_expr_val_num := evaluate_expression(
                        rec.expression, p_step_id, v_key_val, v_subkey_val, v_wpkey_val, p_run_id);
            end if;

            if ((v_expr_val_num = 1) or (rec.is_default = 1 and not(v_is_dec_found))) then
                update wf_decision_option set is_true = 1
                 where wf_decision_id = rec.wf_decision_id
                         and order_number = rec.order_number;

                v_is_dec_found := true;

                if (v_is_inclusive <> 1) then
                    exit;
                end if;
            else
                update wf_decision_option set is_true = 0
                 where wf_decision_id = rec.wf_decision_id
                         and order_number = rec.order_number;
            end if;
        end loop;

        update wf_step set finish_date = current_date where wf_step_id = p_step_id;
    end exec_decision;


    procedure propagate_step_changes(
        p_template_step_id in wf_template_step.wf_template_step_id%type,
        p_step_id in wf_step.wf_step_id%type)
        is
        v_finish_date wf_step.finish_date%type;

        rt_step wf_template_step%rowtype;
    begin
        select finish_date into v_finish_date
          from wf_step where wf_step_id = p_step_id;
        if (v_finish_date is not null) then
-- cant propagate changes to finished step
            return;
        end if;

        select * into rt_step from wf_template_step
         where wf_template_step_id = p_template_step_id;

        update wf_step set
            step_type_id = rt_step.step_type_id,
            title_label_id = rt_step.title_label_id,
            description = rt_step.description,
            plsql_block = rt_step.plsql_block
         where wf_step_id = p_step_id;

        if (rt_step.step_type_id = c_step_type_cfields) then
            delete from wf_step_cfield_xref where wf_step_id = p_step_id;

            insert into wf_step_cfield_xref
            (wf_step_id, config_field_id, order_number)
            select p_step_id, config_field_id, order_number
              from wf_template_step_cfield_xref
             where wf_template_step_id = rt_step.wf_template_step_id;

        elsif (rt_step.step_type_id = c_step_type_ctab) then
            delete from wf_step_cgroup_xref where wf_step_id = p_step_id;

            insert into wf_step_cgroup_xref
            (wf_step_id, config_group_id, order_number)
            select p_step_id, config_group_id, order_number
              from wf_template_step_cgroup_xref
             where wf_template_step_id = rt_step.wf_template_step_id;

        elsif (rt_step.step_type_id = c_step_type_capp) then
            delete from wf_step_capp_xref where wf_step_id = p_step_id;

            insert into wf_step_capp_xref (wf_step_id, config_app_id)
            select p_step_id, config_app_id
              from wf_template_step_capp_xref
             where wf_template_step_id = rt_step.wf_template_step_id;
        end if;

    end propagate_step_changes;

    procedure propagate_decision_changes(
        p_dec_id in wf_template_decision.wf_template_decision_id%type,
        p_run_dec_id in wf_decision.wf_decision_id%type)
    as
        rt_dec wf_template_decision%rowtype;
    begin
        select * into rt_dec from wf_template_decision
         where wf_template_decision_id = p_dec_id;

        update wf_decision set
            description = rt_dec.description,
            is_inclusive_decision = rt_dec.is_inclusive_decision,
            decision_name = rt_dec.decision_name
         where wf_decision_id = p_run_dec_id;
    end propagate_decision_changes;


    function get_proc_progress(p_run_id in wf_workflow.wf_workflow_id%type)
        return integer
    as
        v_steps_finished integer;
    begin
        select count(*) into v_steps_finished
          from wf_step where finish_date is not null and wf_workflow_id = p_run_id;

        return v_steps_finished;
    end get_proc_progress;


    function get_proc_steps_cnt(p_run_id in wf_workflow.wf_workflow_id%type)
        return integer
    as
        v_steps_cnt integer;
    begin
        select count(*) into v_steps_cnt
          from wf_step where wf_workflow_id = p_run_id;

        return v_steps_cnt;
    end get_proc_steps_cnt;


    function get_cur_steps_formatted(p_run_id in wf_workflow.wf_workflow_id%type)
        return clob
    as
        v_state wf_workflow.wf_state_id%type;
        v_result varchar2(10000) := '';
        v_minutes integer;
        v_minutes_chr varchar2(4);
        v_hours integer;
        v_days integer;
        v_formatted varchar2(30);
        v_assigned_exists boolean;
    begin
        select wf_state_id into v_state from wf_workflow where wf_workflow_id = p_run_id;
        if (v_state not in (1, 3)) then
            return null;
        end if;

        for rec in (
            select r.wf_workflow_id, ns.wf_step_id, nvl(l.label_text, ns.step_name) step_name,
                ns.wf_template_step_id,
                round((current_date - nvl(ns.finish_date, r.start_date)) * 24 * 60) awaiting_minutes
              from wf_next_step vwns join wf_step ns
                                          on (ns.wf_step_id = vwns.next_wf_step_id)
              join wf_workflow r on (r.wf_workflow_id = ns.wf_workflow_id)
              left outer join vw_label l on (l.label_id = ns.title_label_id and l.app_lang_id = pkg_sec.get_lang())
             where ns.wf_workflow_id = p_run_id) loop

            v_result := v_result || '<a href="." target="ifrm" onclick="clickGridCell(event); ';
            v_result := v_result || 'showWfStep(''' || rec.wf_workflow_id || ''',''' || rec.wf_step_id || ''',''1'')">';
            v_result := v_result || rec.step_name || '</a> (';

            v_assigned_exists := false;
            for rec_user in (
                select u.user_id, u.un
                  from users u join wf_step_user_xref su on (u.user_id = su.user_id)
                 where su.wf_step_id = rec.wf_step_id) loop

                v_result := v_result
                    || '<a href="." target="ifrm" onclick="clickGridCell(event);user_onclick('''
                    || rec_user.user_id || ''');">' || rec_user.un || '</a>';
                if (length(v_result) > 9000) then
                    v_result := v_result || '...';
                    exit;
                else
                    v_result := v_result || ', ';
                end if;

                v_assigned_exists := true;
            end loop;
            v_result := substr(v_result, 1, length(v_result) - 2);

            if v_assigned_exists then
                v_result := v_result || ')';
            end if;

            if ((rec.awaiting_minutes is not null) and (v_state = 1)) then
                v_minutes := rec.awaiting_minutes;
                v_hours := trunc(v_minutes/60);
                v_minutes_chr := substr(to_char(v_minutes - v_hours * 60, '09'), 2);

                if (v_hours > 24) then
                    v_days := trunc(v_hours/24);
                    v_hours := v_hours - v_days * 24;

                    v_formatted := ', ' || v_days;
                    if (v_days = 1) then
                        v_formatted := v_formatted || ' ' || pkg_label.get_label_system(6074, pkg_sec.get_lang()) || ', ';
                    else
                        v_formatted := v_formatted || ' ' || pkg_label.get_label_system(6075, pkg_sec.get_lang()) || ', ';
                    end if;
                    v_formatted := v_formatted || v_hours || ':' || v_minutes_chr;
                else
                    v_formatted := ', ' || v_hours || ':' || v_minutes_chr;
                end if;
            end if;

            v_result := v_result || v_formatted || '<br>';
        end loop;

        return v_result;
    end get_cur_steps_formatted;


    function delete_running_proc(p_run_id in wf_workflow.wf_workflow_id%type) return number
        is
    begin
        delete from wf_decision_option where wf_decision_id in (
            select wf_decision_id from wf_step where wf_workflow_id = p_run_id);
        delete from wf_step where wf_workflow_id = p_run_id;
        delete from wf_workflow where wf_workflow_id = p_run_id;
        return sql%rowcount;
    end delete_running_proc;

    procedure delete_running_proc(p_run_id in wf_workflow.wf_workflow_id%type)
        is
        v_ignored number;
    begin
        v_ignored := delete_running_proc(p_run_id);
    end delete_running_proc;


    function get_prev_step(p_current_step in wf_step.wf_step_id%type) return wf_step.wf_step_id%type
    as
        v_prev_step_id wf_step.wf_step_id%type;
        v_decision_count number;
    begin
        select count(1)
          into v_decision_count
          from wf_decision_option
         where next_wf_step_id = p_current_step;

        if v_decision_count > 0 then
            return v_prev_step_id;
        end if;

        select case when count(1) = 1 then max(prev_step.wf_step_id) else null end as prev_step_id
          into v_prev_step_id
          from wf_step prev_step
          join wf_next_step next_step on next_step.next_wf_step_id = prev_step.next_wf_step_id
          join wf_step_user_xref prev_rstep_assigned on prev_rstep_assigned.wf_step_id = prev_step.wf_step_id
          join wf_step_user_xref cur_rstep_assigned on cur_rstep_assigned.wf_step_id = prev_step.next_wf_step_id
                                                   and prev_rstep_assigned.user_id = cur_rstep_assigned.user_id
         where prev_step.next_wf_step_id = p_current_step
           and prev_step.step_type_id in (c_step_type_cfields,
                                          c_step_type_ctab,
                                          c_step_type_capp,
                                          c_step_type_new_trackor,
                                          c_step_type_wp_tasks,
                                          c_step_type_single_wp_task,
                                          c_step_type_instr);

        return v_prev_step_id;
    end get_prev_step;


    function prev_step(p_current_step in wf_step.wf_step_id%type) return wf_step.wf_step_id%type
    is
        v_prev_step_id wf_step.wf_step_id%type;
        v_wf_id wf_step.wf_workflow_id%type;
    begin
        v_prev_step_id := get_prev_step(p_current_step);

        if v_prev_step_id is null then
            raise_application_error(-20000, pkg_label.get_label_system(18054));
        end if;

        select wf_workflow_id
          into v_wf_id
          from wf_step
         where wf_step_id = v_prev_step_id;

        update wf_step
           set finish_date = null
         where wf_step_id = v_prev_step_id;

        delete from wf_next_step
         where wf_workflow_id = v_wf_id
           and next_wf_step_id = p_current_step;

        insert into wf_next_step(wf_workflow_id, wf_step_id, next_wf_step_id)
        values (v_wf_id, p_current_step, v_prev_step_id);

        return v_prev_step_id;
    end prev_step;


    procedure assign_step_before(
        p_next_step_id in wf_template_step.wf_template_step_id%type,
        p_tmpl_step_id in wf_template_step.wf_template_step_id%type)
        is
        v_tmpl_id wf_template.wf_template_id%type;
        v_first_step_id wf_template_step.wf_template_step_id%type;
        v_key_val_cfid wf_step.key_value_field_id%type;
        v_step_from_decision number;
    begin
        select count(*) into v_step_from_decision
          from wf_template_decision_option
         where next_wf_template_step_id = p_next_step_id
                 and next_wf_template_step_id <> (
             select first_template_step_id
               from wf_template t join wf_template_step ts on (ts.wf_template_id = t.wf_template_id)
              where ts.wf_template_step_id = p_next_step_id);

        if (v_step_from_decision > 0) then
            raise_application_error(-20000, pkg_label.get_label_system(17753));
        end if;

        select t.wf_template_id, t.first_template_step_id
          into v_tmpl_id, v_first_step_id
          from wf_template_step ts join wf_template t
                                        on (t.wf_template_id = ts.wf_template_id)
         where ts.wf_template_step_id = p_next_step_id;

        select key_value_field_id into v_key_val_cfid
          from wf_template_step where wf_template_step_id = p_tmpl_step_id;

        if (p_next_step_id = v_first_step_id) then
            update wf_template set first_template_step_id = p_tmpl_step_id
             where wf_template_id = v_tmpl_id;
        end if;

        for rec_in_steps in (
            select wf_template_step_id from wf_template_step
             where next_wf_template_step_id = p_next_step_id) loop
            update wf_template_step set next_wf_template_step_id = p_tmpl_step_id
             where wf_template_step_id = rec_in_steps.wf_template_step_id;
        end loop;

        update wf_template_step set next_wf_template_step_id = p_next_step_id
         where wf_template_step_id = p_tmpl_step_id;

    end assign_step_before;


    procedure delete_step(p_step_id in wf_template_step.wf_template_step_id%type)
        is
        v_tmpl_id wf_template.wf_template_id%type;
        v_first_step_id wf_template_step.next_wf_template_step_id%type;
        v_next_step_id wf_template_step.next_wf_template_step_id%type;
        v_dec_id wf_template_step.wf_template_decision_id%type;
        v_dec_options_cnt integer;
        v_is_loop wf_template_step.is_loop%type;
    begin
        select t.wf_template_id, t.first_template_step_id,
            ts.next_wf_template_step_id, ts.is_loop, ts.wf_template_decision_id, (
            select count(*) from wf_template_decision_option
             where wf_template_decision_id = ts.wf_template_decision_id)
          into v_tmpl_id, v_first_step_id, v_next_step_id, v_is_loop, v_dec_id, v_dec_options_cnt
          from wf_template_step ts join wf_template t
                                        on (t.wf_template_id = ts.wf_template_id)
         where ts.wf_template_step_id = p_step_id;

        if (v_dec_options_cnt > 1) then
            raise_application_error(-20000, pkg_label.get_label_system(17754));
        elsif (v_dec_id is not null) then
            begin
                select next_wf_template_step_id into v_next_step_id
                  from wf_template_decision_option where wf_template_decision_id = v_dec_id;
            exception
                when no_data_found then
                    null;
            end;
        end if;

        delete from wf_template_decision
         where wf_template_decision_id = v_dec_id;

        delete from wf_template_step where wf_template_decision_id = v_dec_id;

        update wf_template_decision_option
           set next_wf_template_step_id = v_next_step_id
         where next_wf_template_step_id = p_step_id;

        update wf_template_step
           set next_wf_template_step_id = v_next_step_id, is_loop = v_is_loop
         where next_wf_template_step_id = p_step_id
                 and v_next_step_id <> wf_template_step_id;

        if (v_first_step_id = p_step_id) then
            update wf_template set first_template_step_id = v_next_step_id
             where wf_template_id = v_tmpl_id;
        end if;

        delete from wf_template_step where wf_template_step_id = p_step_id;
    end delete_step;


    function insert_decision(
        p_tmpl_id wf_template.wf_template_id%type,
        p_prev_step_id in wf_template_step.wf_template_step_id%type,
        p_dec_name in wf_template_decision.decision_name%type,
        p_desc in wf_template_decision.description%type,
        p_is_inclusive in wf_template_decision.is_inclusive_decision%type)
        return wf_template_step.wf_template_step_id%type
        is
        v_dec_id wf_template_decision.wf_template_decision_id%type;
        v_prev_dec_id wf_template_decision.wf_template_decision_id%type;
        v_dec_step_id wf_template_step.wf_template_step_id%type;
        v_next_step_id wf_template_step.next_wf_template_step_id%type;
    begin
        if (p_prev_step_id is not null) then
            select next_wf_template_step_id, wf_template_decision_id
              into v_next_step_id, v_prev_dec_id
              from wf_template_step where wf_template_step_id = p_prev_step_id;
        end if;

        if (v_prev_dec_id is not null) then
            raise_application_error(-20000, pkg_label.get_label_system(17755));
        end if;

        insert into wf_template_step(wf_template_id) values(p_tmpl_id)
        returning wf_template_step_id into v_dec_step_id;

        insert into wf_template_decision(description, is_inclusive_decision, decision_name, wf_template_step_id)
        values(p_desc, p_is_inclusive, p_dec_name, v_dec_step_id)
        returning wf_template_decision_id into v_dec_id;

        update wf_template_step set wf_template_decision_id = v_dec_id
         where wf_template_step_id = v_dec_step_id;

        insert into wf_template_decision_option(wf_template_decision_id,
                                                next_wf_template_step_id, is_default, option_name)
        values(v_dec_id, v_next_step_id, 1, 'DEFAULT');

        update wf_template_step set next_wf_template_step_id = v_dec_step_id
         where wf_template_step_id = p_prev_step_id;

        return v_dec_step_id;
    end insert_decision;

    function delete_template(p_tmpl_id wf_template.wf_template_id%type) return number
        is
        v_wf_count number;
        v_sec_id sec_group_program.sec_group_program_id%type;
        v_deleted_rows number;
        v_pid wf_template.program_id%type;
    begin
        select count(*) into v_wf_count from wf_workflow
         where wf_template_id = p_tmpl_id;

        if (v_wf_count > 0) then
            raise_application_error(-20000, pkg_label.get_label_system_wrapped(17756));
        end if;

        delete from wf_template_decision
         where wf_template_step_id in (
             select wf_template_step_id
               from wf_template_step
              where wf_template_id = p_tmpl_id);

        select program_id into v_pid
          from wf_template where wf_template_id = p_tmpl_id;

        select sg.sec_group_program_id into v_sec_id
          from sec_group_program sg, wf_template wft
         where sg.security_group = 'WF ' || wft.template_name
                 and wft.wf_template_id = p_tmpl_id
                 and sg.program_id = v_pid;

        pkg_sec_priv_program.delete_sec_group(v_sec_id);

        delete from rule_id_num
         where id_num = p_tmpl_id
           and rule_id in (select rule_id from rule
                            where rule_type_id in (pkg_ruleator.c_type_wf_started,
                                                   pkg_ruleator.c_type_wf_updated,
                                                   pkg_ruleator.c_type_wf_finished,
                                                   pkg_ruleator.c_type_wf_deleted));

        delete from wf_template where wf_template_id = p_tmpl_id;
        v_deleted_rows := sql%rowcount;

        return v_deleted_rows;
    end delete_template;

    procedure delete_template(p_tmpl_id wf_template.wf_template_id%type)
        is
        v_ignored number;
    begin
        v_ignored := delete_template(p_tmpl_id);
    end delete_template;

    procedure copy_plsql_step_to_all_wfs(p_wf_template_step_id in wf_template_step.wf_template_step_id%type,
                                         p_uid in users.user_id%type)
    as
        v_plsql wf_template_step.plsql_block%type;
    begin
        select plsql_block into v_plsql
          from wf_template_step
         where wf_template_step_id = p_wf_template_step_id
                 and step_type_id = c_step_type_plsql ;

        update wf_step set plsql_block = v_plsql
         where wf_step_id in (
             select s.wf_step_id
               from wf_step s
               join wf_workflow w on (w.wf_workflow_id = s.wf_workflow_id)
              where s.wf_template_step_id = p_wf_template_step_id
                      and w.wf_state_id not in (c_state_finished, c_state_abandoned)
                      and s.step_type_id = c_step_type_plsql);

    end copy_plsql_step_to_all_wfs;

    procedure copy_plsql_step_to_wfs(p_wf_template_step_id in wf_template_step.wf_template_step_id%type,
                                     p_wf_ids in tableOfNum,
                                     p_uid in users.user_id%type)
    as
        v_plsql wf_template_step.plsql_block%type;
    begin
        select plsql_block into v_plsql
          from wf_template_step
         where wf_template_step_id = p_wf_template_step_id
                 and step_type_id = c_step_type_plsql ;

        update wf_step set plsql_block = v_plsql
         where wf_step_id in (
             select s.wf_step_id
               from wf_step s
               join wf_workflow w on (w.wf_workflow_id = s.wf_workflow_id)
              where s.wf_template_step_id = p_wf_template_step_id
                      and w.wf_state_id not in (c_state_finished, c_state_abandoned)
                      and s.step_type_id = c_step_type_plsql
                      and w.wf_workflow_id in (select column_value from table(p_wf_ids)));

    end copy_plsql_step_to_wfs;

    function get_cur_steps_form_formatted(p_wfid in wf_workflow.wf_workflow_id%type)
        return clob
    as
        v_state wf_workflow.wf_state_id%type;
        v_result varchar2(10000) := '';
        v_minutes integer;
        v_minutes_chr varchar2(4);
        v_hours integer;
        v_days integer;
        v_formatted varchar2(30);
        v_assigned_exists boolean;
        v_lang_id number := pkg_sec.get_lang();
    begin
        select wf_state_id into v_state from wf_workflow where wf_workflow_id = p_wfid;
        if (v_state not in (1, 3)) then
            return null;
        end if;

        for rec in (
            select
                r.wf_workflow_id,
                ns.wf_step_id,
                nvl(l.label_text, ns.step_name) step_name,
                ns.wf_template_step_id,
                round((current_date - nvl(ns.finish_date, r.start_date)) * 24 * 60) awaiting_minutes
              from wf_next_step vwns
              join wf_step ns on (ns.wf_step_id = vwns.next_wf_step_id)
              join wf_workflow r on (r.wf_workflow_id = ns.wf_workflow_id)
              left outer join vw_label l on (l.label_id = ns.title_label_id and l.app_lang_id = v_lang_id)
             where ns.wf_workflow_id = p_wfid)
        loop
            v_result := rec.step_name || ' (';

            v_assigned_exists := false;
            for rec_user in (
                select
                    u.user_id,
                    u.un
                  from
                      users u
                      join wf_step_user_xref su on (u.user_id = su.user_id)
                 where su.wf_step_id = rec.wf_step_id)
            loop
                v_result := v_result || rec_user.un || ', ';
                v_assigned_exists := true;
            end loop;
            v_result := substr(v_result, 1, length(v_result) - 2);

            if v_assigned_exists then
                v_result := v_result || ')';
            end if;

            if ((rec.awaiting_minutes is not null) and (v_state = 1)) then
                v_minutes := rec.awaiting_minutes;
                v_hours := trunc(v_minutes/60);
                v_minutes_chr := substr(to_char(v_minutes - v_hours * 60, '09'), 2);

                if (v_hours > 24) then
                    v_days := trunc(v_hours/24);
                    v_hours := v_hours - v_days * 24;
                    v_formatted := ', ' || v_days;
                    if (v_days = 1) then
                        v_formatted := v_formatted || ' ' || pkg_label.get_label_system(6074, v_lang_id) || ', ';
                    else
                        v_formatted := v_formatted || ' ' || pkg_label.get_label_system(6075, v_lang_id) || ', ';
                    end if;
                    v_formatted := v_formatted || v_hours || ':' || v_minutes_chr;
                else
                    v_formatted := ', ' || v_hours || ':' || v_minutes_chr;
                end if;
            end if;
            v_result := v_result || v_formatted;
        end loop;
        return v_result;
    end get_cur_steps_form_formatted;

    function start_mass_wfs_for_tids(p_tids in tableOfNum,
                                     p_wftid in wf_template.wf_template_id%type,
                                     p_owner_uid in users.user_id%type default null,
                                     p_owner_cfid in config_field.config_field_id%type default null) return tableofnum as

        v_desc_cfid config_field.config_field_id%type;
        v_desc_cf_ttid xitor_type.xitor_type_id%type;
        v_step_ids list_id;
        v_wf_id wf_workflow.wf_workflow_id%type;
        v_step_type_id wf_step.step_type_id%type;
        v_step_cfs_cnt number;
        v_step_cfid wf_step_cfield_xref.config_field_id%type;
        v_wf_key_cfid wf_workflow.key_value_field_id%type;
        v_next_step_ids list_id;

        v_owner_cf_ttid config_field.xitor_type_id%type;

        ret_wfids tableofnum := new tableofnum();
    begin
        begin
            select desc_cf_id, f.xitor_type_id into v_desc_cfid, v_desc_cf_ttid
              from wf_template w join config_field f on (f.config_field_id = w.desc_cf_id)
             where wf_template_id = p_wftid;
        exception
            when no_data_found then
                v_desc_cfid := null;
                v_desc_cf_ttid := null;
        end;

        begin
            select xitor_type_id into v_owner_cf_ttid
              from config_field where config_field_id = p_owner_cfid;
        exception
            when no_data_found then
                v_owner_cf_ttid := null;
        end;

        for rec in (
            select wf_owner_id, xitor_id, wf_name, wf_desc from (select
                t.xitor_id,
                pkg_config_field_rpt.getvalstr(t.xitor_id, 'XITOR_KEY') as wf_name,
                case
                    when v_desc_cfid is not null and t.xitor_type_id = v_desc_cf_ttid
                        then substr(pkg_config_field_rpt.getvalstrbyid(t.xitor_id, v_desc_cfid), 1, 4000)
                    when (v_desc_cfid is not null and t.xitor_type_id <> v_desc_cf_ttid
                        and (select count(parent_id)
                               from ancestor
                              where child_id = t.xitor_id
                                      and c_xitor_type_id = t.xitor_type_id
                                      and p_xitor_type_id = v_desc_cf_ttid) = 1
                        )
                        then substr(pkg_config_field_rpt.getValStrByParentID(t.xitor_id, t.xitor_type_id, v_desc_cf_ttid, v_desc_cfid), 1, 4000)
                    else
                        null
                    end as wf_desc,
                case
                    when p_owner_uid is not null
                        then to_number(p_owner_uid)
                    when p_owner_cfid is not null and t.xitor_type_id = v_owner_cf_ttid
                        then (select user_id from users where xitor_id = pkg_config_field_rpt.getValNumNLByID(t.xitor_id, p_owner_cfid))
                    when (p_owner_cfid is not null and t.xitor_type_id <> v_owner_cf_ttid
                        and (select count(parent_id)
                               from ancestor
                              where child_id = t.xitor_id
                                      and c_xitor_type_id = t.xitor_type_id
                                      and p_xitor_type_id = v_owner_cf_ttid) = 1
                        )
                        then (select user_id from users where xitor_id = pkg_config_field_rpt.getValNumNLByParentID(t.xitor_id, t.xitor_type_id, v_owner_cf_ttid, p_owner_cfid))
                    else
                        null
                    end as wf_owner_id
                             from
                                 xitor t
                            where t.xitor_id in (select column_value from table(p_tids))
            ) where wf_owner_id is not null
            ) loop
            v_step_ids := start_new_process_with_key_val(p_wftid, rec.wf_owner_id, rec.xitor_id, rec.wf_name, rec.wf_desc);

            if v_step_ids is not null and v_step_ids.count > 0 then
                select wf_workflow_id,step_type_id into v_wf_id,v_step_type_id
                  from wf_step where wf_step_id = v_step_ids(1);

                ret_wfids.extend();
                ret_wfids(ret_wfids.count) := v_wf_id;
                if v_step_type_id = c_step_type_cfields then
                    select count(field_name) into v_step_cfs_cnt
                      from wf_step_cfield_xref where wf_step_id = v_step_ids(1);

                    if v_step_cfs_cnt = 1 then
                        begin
                            select config_field_id into v_step_cfid
                              from wf_step_cfield_xref where wf_step_id = v_step_ids(1) and rownum < 2;
                        exception
                            when no_data_found then
                                v_step_cfid := null;
                        end;

                        if v_step_cfid is not null
                            and v_wf_key_cfid is not null
                            and v_step_cfid = v_wf_key_cfid
                        then
                            v_next_step_ids := next_step(v_wf_id, v_step_ids(1));
                        end if;
                    end if;
                end if;
            end if;
        end loop;

        return ret_wfids;
    end start_mass_wfs_for_tids;

    procedure clone_wf_template_steps(p_source_templ_id in wf_template.wf_template_id%type,
                                      p_dest_templ_id in wf_template.wf_template_id%type)
    as
        v_step_id wf_template_step.wf_template_step_id%type;
        v_des_id wf_template_decision.wf_template_decision_id%type;
        v_next_step_id wf_template_step.next_wf_template_step_id%type;

        type table_of_numbers is table of number index by pls_integer;
        step_ids table_of_numbers;

        cursor cur_steps(p_source_templ_id in wf_template.wf_template_id%type) is
            select s.wf_template_step_id,
                   s.step_type_id,
                   s.title_label_id,
                   s.step_name,
                   s.description step_desc,
                   s.plsql_block,
                   s.key_value_field_id,
                   s.next_wf_template_step_id,
                   s.is_can_return,
                   s.is_loop,
                   d.wf_template_decision_id,
                   d.description des_desc,
                   d.is_inclusive_decision,
                   d.decision_name,
                   s.block_pos_x,
                   s.block_pos_y,
                   s.disable_navigation,
                   s.status_config_field_id
              from wf_template_step s
              left join wf_template_decision d on (d.wf_template_decision_id = s.wf_template_decision_id)
             where s.wf_template_id = p_source_templ_id;

    begin
        --Copy steps and decisions
        for rec in cur_steps(p_source_templ_id) loop
            insert into wf_template_step(
                step_name,
                step_type_id,
                title_label_id,
                wf_template_id,
                description,
                plsql_block,
                key_value_field_id,
                is_can_return,
                is_loop,
                block_pos_x,
                block_pos_y,
                disable_navigation,
                status_config_field_id
            ) values (
                rec.step_name,
                rec.step_type_id,
                rec.title_label_id,
                p_dest_templ_id,
                rec.step_desc,
                rec.plsql_block,
                rec.key_value_field_id,
                rec.is_can_return,
                rec.is_loop,
                rec.block_pos_x,
                rec.block_pos_y,
                rec.disable_navigation,
                rec.status_config_field_id
            ) returning wf_template_step_id
                   into v_step_id;

            step_ids(rec.wf_template_step_id) := v_step_id;

            insert into wf_template_step_user_xref(wf_template_step_id, user_id)
            select v_step_id,
                   user_id
              from wf_template_step_user_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_template_step_role_xref(wf_template_step_id, sec_role_id)
            select v_step_id,
                   sec_role_id
              from wf_template_step_role_xref
             where wf_template_step_id = rec.wf_template_step_id;

            if (rec.wf_template_decision_id is not null) then
                insert into wf_template_decision(
                    description,
                    is_inclusive_decision,
                    decision_name,
                    wf_template_step_id
                ) values(
                    rec.des_desc,
                    rec.is_inclusive_decision,
                    rec.decision_name,
                    v_step_id
                ) returning wf_template_decision_id
                       into v_des_id;

                update wf_template_step
                   set wf_template_decision_id = v_des_id
                 where wf_template_step_id = v_step_id;

                insert into wf_template_decision_option(
                    wf_template_decision_id,
                    order_number,
                    expression,
                    is_default,
                    option_name,
                    goto_end
                ) select v_des_id,
                         order_number,
                         expression,
                         is_default,
                         option_name,
                         goto_end
                    from wf_template_decision_option
                   where wf_template_decision_id = rec.wf_template_decision_id;
            end if;

            --Copy wf_step_*_XREF tables
            insert into wf_template_step_capp_xref(wf_template_step_id, config_app_id)
            select v_step_id,
                   config_app_id
              from wf_template_step_capp_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_template_step_cgroup_xref(wf_template_step_id, config_group_id, order_number)
            select v_step_id,
                   config_group_id,
                   order_number
              from wf_template_step_cgroup_xref
             where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_template_step_cfield_xref(
                wf_template_step_id,
                config_field_id,
                order_number,
                field_name,
                template_task_id
            ) select v_step_id,
                     config_field_id,
                     order_number,
                     field_name,
                     template_task_id
                from wf_template_step_cfield_xref
               where wf_template_step_id = rec.wf_template_step_id;

            insert into wf_template_step_ttype_xref (wf_template_step_id, trackor_type_id)
            select v_step_id,
                   trackor_type_id
              from wf_template_step_ttype_xref
             where wf_template_step_id = rec.wf_template_step_id;
        end loop;

        for rec in cur_steps(p_source_templ_id)
        loop
            --Fill wf_step.next_wf_step_id
            v_step_id := step_ids(rec.wf_template_step_id);
            if (rec.next_wf_template_step_id is not null) then
                v_next_step_id := step_ids(rec.next_wf_template_step_id);

                update wf_template_step
                   set next_wf_template_step_id = v_next_step_id
                 where wf_template_step_id = v_step_id;
            end if;

            --Fill wf_decision_option.next_wf_step_id
            if (rec.wf_template_decision_id is not null) then
                select td.wf_template_decision_id
                  into v_des_id
                  from wf_template_decision td
                  join wf_template_step ts on (td.wf_template_decision_id = ts.wf_template_decision_id)
                 where ts.wf_template_step_id = v_step_id;

                for rec_dec_opt in (select order_number,
                                           next_wf_template_step_id
                                      from wf_template_decision_option
                                     where wf_template_decision_id = rec.wf_template_decision_id
                                       and next_wf_template_step_id is not null)
                loop
                    v_next_step_id := step_ids(rec_dec_opt.next_wf_template_step_id);
                    update wf_template_decision_option
                       set next_wf_template_step_id = v_next_step_id
                     where order_number = rec_dec_opt.order_number
                       and wf_template_decision_id = v_des_id;
                end loop;
            end if;
        end loop;

        --Fill first step
        select ts.wf_template_step_id
          into v_step_id
          from wf_template_step ts
          join wf_template t on (t.first_template_step_id = ts.wf_template_step_id)
         where ts.wf_template_id = p_source_templ_id;

        v_step_id := step_ids(v_step_id);

        update wf_template
           set first_template_step_id = v_step_id
         where wf_template_id = p_dest_templ_id;
    end clone_wf_template_steps;


    function can_navigate_to_prev_step(p_current_step in wf_step.wf_step_id%type) return boolean as
    begin
        return get_prev_step(p_current_step) is not null;
    end can_navigate_to_prev_step;
end pkg_wf;
/