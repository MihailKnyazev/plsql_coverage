CREATE OR REPLACE PACKAGE BODY PKG_FIMP_RUN 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
    --TODO remove with Imp-121196
    c_is_compare boolean;

   /**
    * This procedure insert record to imp_run_entity_pk that's already in DB (xitor table)
    *
    * @param p_imp_spec_id the import specification id
    * @param p_imp_run_id the import run id
    * @param p_imp_entity_id the entity id
    * @param p_user_ent_sql SQL statement to identify an entity in the database   
    */ 
    procedure fill_existing_entity_pks (
        p_imp_spec_id in imp_spec.imp_spec_id%type,
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_user_ent_sql in imp_entity.sql_text%type);

   /**
    * This procedure logging error for existing entities,
    * triggered if import action id = 2(update only)
    *
    * @param p_imp_run_id the import run id
    * @param p_imp_entity_id the import entity id
    * @param p_entity_name the import entity name
    */
    procedure errors_for_existing_entities(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type, 
        p_entity_name in imp_entity.entity_name%type);

   /**
    * This procedure create new entities which not presented in xitor table,
    * triggered if import_cation_id = 3 (insert/update)
    *
    * @param p_imp_run_id the import id
    * @param p_imp_entity_id the import entity id
    * @param p_xitor_type_id the xitor type id
    */    
    procedure new_entities(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type);

   /**
    * This procedure creating new trackor for import record
    * and delete existing records from imp_run_entity_pk,
    * triggered if import_cation_id = 4 (insert only)
    *
    * @param p_imp_run_id the import run id
    * @param p_imp_entity_id the import entity id
    * @param p_xitor_type_id the xitor type id
    */   
    procedure handle_insert_only(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type);

   /**
    * This procedure copy data to fimp_run_entity_pk to next compare
    *
    * @param p_imp_run_id the import id
    */     
    procedure prepare_to_compare_data(p_imp_run in imp_run.imp_run_id%type);

   /**
    * This procedure creating new trackor for import record
    *
    * @param p_imp_run_id the import run id
    * @param p_imp_entity_id the import entity id
    * @param p_xitor_type_id the xitor type id
    * @param p_row_num the row number from csv file
    */
    procedure create_entity(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type,
        p_row_num in imp_run_grid_incr.row_num%type,
        p_irun in pkg_imp_run.cur_imp_run%rowtype);

   /**
    * This function returns first column from sql query to search entity
    *
    * @param p_imp_run_id the import run id
    * @param p_sql SQL statement to identify an entity in the database   
    */
    function get_entity_sql_1st_col_name(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type, 
        p_sql in clob) return varchar2;


    --TODO remove with Imp-121196
    procedure log_error(
        p_imp_run_id in fimp_run_error.imp_run_id%type,
        p_error_msg in fimp_run_error.error_msg%type,
        p_err_type_id in fimp_run_error.imp_error_type_id%type,
        p_row_num in fimp_run_error.row_num%type,
        p_sql in fimp_run_error.sql_text%type default null)
    as
        pragma autonomous_transaction;
    begin
        insert into fimp_run_error(imp_run_id, error_msg, sql_text, imp_error_type_id, row_num)
        values (p_imp_run_id, p_error_msg, p_sql, p_err_type_id, p_row_num);

        commit;
    end log_error;


    procedure close_cursor(p_cursor in out number) as 
    begin
        if dbms_sql.is_open(p_cursor) then
            dbms_sql.close_cursor(p_cursor);
        end if;   
    end close_cursor;


    procedure start_fimp_run_log(
        p_imp_run_id in fimp_run_entity_pk_log.imp_run_id%type,
        p_imp_entity_id in fimp_run_entity_pk_log.entity_id%type,
        p_sql in fimp_run_entity_pk_log.sql_text%type)
    as
        pragma autonomous_transaction;
    begin    
        if c_is_compare then
            insert into fimp_run_entity_pk_log(imp_run_id, entity_id, sql_text, start_ts) 
                values (p_imp_run_id, p_imp_entity_id, p_sql, current_date);

            delete from fimp_run_entity_pk 
             where imp_run_id = p_imp_run_id and imp_entity_id = p_imp_entity_id;

            commit;            
        end if;    
    end start_fimp_run_log;  


    procedure finish_fimp_run_log(
        p_imp_run_id in fimp_run_entity_pk_log.imp_run_id%type,
        p_imp_entity_id in fimp_run_entity_pk_log.entity_id%type)
    as
        pragma autonomous_transaction;
    begin 
        if c_is_compare then   
            update fimp_run_entity_pk_log set finish_ts = current_date
             where imp_run_id = p_imp_run_id 
               and entity_id = p_imp_entity_id;
            commit;
        end if;             
    end finish_fimp_run_log;           


    procedure set_cnt_pk_find(
        p_imp_run_id in fimp_run_entity_pk_log.imp_run_id%type,
        p_imp_entity_id in fimp_run_entity_pk_log.entity_id%type,
        p_cnt in fimp_run_entity_pk_log.cnt_find%type)
    as
        pragma autonomous_transaction;
    begin    
        if c_is_compare then
            update fimp_run_entity_pk_log set cnt_find = p_cnt 
             where imp_run_id = p_imp_run_id
               and entity_id = p_imp_entity_id;
            commit;                 
        end if;    
    end set_cnt_pk_find; 


    procedure set_cnt_pk_new(
        p_imp_run_id in fimp_run_entity_pk_log.imp_run_id%type,
        p_imp_entity_id in fimp_run_entity_pk_log.entity_id%type,
        p_cnt in fimp_run_entity_pk_log.cnt_new%type)
    as
        pragma autonomous_transaction;
    begin    
        if c_is_compare then
            update fimp_run_entity_pk_log set cnt_new = p_cnt 
             where imp_run_id = p_imp_run_id
               and entity_id = p_imp_entity_id;
            commit;
        end if;                             
    end set_cnt_pk_new;


    procedure fill_single_entity_pks(
        p_cur_imp_run in pkg_imp_run.cur_imp_run%rowtype,
        p_imp_entity in pkg_imp_run.cur_imp_entity%rowtype,
        p_is_compare in boolean) --TODO remove with Imp-121196
    as
        v_error_message imp_run_error.error_msg%type;
    begin
        --TODO remove with Imp-121196
        c_is_compare := p_is_compare;

        start_fimp_run_log(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, p_imp_entity.sql_text); --TODO remove with Imp-121196

        fill_existing_entity_pks(p_cur_imp_run.imp_spec_id, p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, p_imp_entity.sql_text);

        if p_cur_imp_run.imp_action_id = pkg_imp_run.c_imp_action_update then
            if not c_is_compare then--TODO remove with Imp-121196
                errors_for_existing_entities(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, p_imp_entity.entity_name);
            end if;

        elsif p_cur_imp_run.imp_action_id = pkg_imp_run.c_imp_action_insert_update then
            new_entities(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, p_imp_entity.xitor_type_id);

        elsif p_cur_imp_run.imp_action_id = pkg_imp_run.c_imp_action_insert then
            handle_insert_only(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id, p_imp_entity.xitor_type_id);
        end if;

        finish_fimp_run_log(p_cur_imp_run.imp_run_id, p_imp_entity.imp_entity_id); --TODO remove with Imp-121196

        prepare_to_compare_data(p_cur_imp_run.imp_run_id);
    exception
        when others then 
            v_error_message := dbms_utility.format_error_backtrace || dbms_utility.format_error_stack;
            --TODO remove with Imp-121196
            if c_is_compare then
                --log error and go to next without raise
                log_error(p_cur_imp_run.imp_run_id, v_error_message, pkg_imp_run.c_et_unknown, 0, null);
            else
                raise_application_error(-20000, v_error_message);--return error in calling procedure
            end if;
    end fill_single_entity_pks;


    --TODO remove with Imp-121196
    procedure change_entity_sql(
        p_user_ent_sql in out nocopy imp_entity.sql_text%type, 
        p_imp_spec_id in imp_spec.imp_spec_id%type,
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type) as 
    begin
        p_user_ent_sql := pkg_imp_run.ireplace(p_user_ent_sql, ';', '');

        for rec_param_value in                           
            (select iep.parameter_value, iep.sql_parameter, iep.imp_column_id, ic.name as ic_name,
                    irg.col_num
               from imp_entity_param iep
               left join imp_column ic on iep.imp_column_id = ic.imp_column_id 
                                      and ic.imp_spec_id = p_imp_spec_id
               left join imp_run_grid_incr irg on irg.data = ic.name 
                                              and irg.imp_run_id = p_imp_run_id
              where iep.imp_entity_id = p_imp_entity_id
                and (irg.col_num is not null or iep.parameter_value is not null)
             )
        loop
            --replace parameter name in the SQL (':p1' to :p1)
            p_user_ent_sql := pkg_imp_run.ireplace(p_user_ent_sql, ''':' || rec_param_value.sql_parameter || '''', ':' || rec_param_value.sql_parameter || ''); 
        end loop;
    end change_entity_sql;


    function get_entity_sql_1st_col_name(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type, 
        p_sql in clob) return varchar2
    is
        v_cursor number;
        v_cols_description dbms_sql.desc_tab;
        v_cols_cnt number;
        v_errmsg varchar2(4000);
    begin
        v_cursor := dbms_sql.open_cursor;
        dbms_sql.parse(v_cursor, p_sql, dbms_sql.native);
        dbms_sql.describe_columns(v_cursor, v_cols_cnt, v_cols_description);
        dbms_sql.close_cursor(v_cursor); 
        return v_cols_description(1).col_name;

    exception 
        when others then
            close_cursor(v_cursor);
            v_errmsg := dbms_utility.format_error_backtrace || dbms_utility.format_error_stack ;
            raise_application_error(-20000, v_errmsg);

    end get_entity_sql_1st_col_name;


    procedure fill_existing_entity_pks (
        p_imp_spec_id in imp_spec.imp_spec_id%type,
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_user_ent_sql in imp_entity.sql_text%type)
    as
        v_full_ent_sql clob;
        v_param_vals_array dbms_sql.varchar2_table;
        v_row_nums_array  dbms_sql.number_table;
        v_rows_processed number;
        v_cursor number;
        v_user_ent_sql imp_entity.sql_text%type;
        v_errmsg varchar2(4000);
        v_user_id users.user_id%type;
        v_program_id program.program_id%type;
        v_start_row imp_run.start_row%type;
    begin
        v_user_ent_sql := p_user_ent_sql;

        --TODO remove with Imp-121196
        change_entity_sql(v_user_ent_sql, p_imp_spec_id, p_imp_run_id, p_imp_entity_id);

        pkg_str.append_line(v_full_ent_sql, 'insert into imp_run_entity_pk(imp_run_id, imp_entity_id, row_num, pk, is_inserted)');
        pkg_str.append_line(v_full_ent_sql, 'select :p_imp_run_id, :p_imp_entity_id, :p_static_row_num, ' || get_entity_sql_1st_col_name(p_imp_run_id, p_imp_entity_id, v_user_ent_sql) || ', 0'); 
        pkg_str.append_line(v_full_ent_sql, 'from (' || v_user_ent_sql || ')');

        v_cursor := dbms_sql.open_cursor;
        dbms_sql.parse(v_cursor, v_full_ent_sql, dbms_sql.native);

        select p.user_id, p.program_id, r.start_row
          into v_user_id, v_program_id, v_start_row
          from process p, imp_run r
         where p.process_id = r.process_id 
           and r.imp_run_id = p_imp_run_id;

        select row_num bulk collect into v_row_nums_array
          from imp_run_grid_incr irg
         where imp_run_id = p_imp_run_id 
           and row_num > v_start_row and col_num = 1
         order by row_num;

        dbms_sql.bind_array(v_cursor, 'p_static_row_num', v_row_nums_array);
        dbms_sql.bind_variable(v_cursor, 'p_imp_run_id', p_imp_run_id);
        dbms_sql.bind_variable(v_cursor, 'p_imp_entity_id', p_imp_entity_id);

        for rec_param_value in (select iep.parameter_value, iep.sql_parameter, irg.col_num
                                  from imp_entity_param iep
                                  left join imp_column ic 
                                    on iep.imp_column_id = ic.imp_column_id and ic.imp_spec_id = p_imp_spec_id
                                  left join imp_run_grid_incr irg 
                                    on irg.data = ic.name and irg.row_num = v_start_row and irg.imp_run_id = p_imp_run_id
                                 where iep.imp_entity_id = p_imp_entity_id) loop

            if rec_param_value.parameter_value is not null then --bind constant val
                if rec_param_value.parameter_value = 'USER_ID' then
                    dbms_sql.bind_variable(v_cursor, rec_param_value.sql_parameter, v_user_id);

                elsif rec_param_value.parameter_value = 'PROGRAM_ID' then
                    dbms_sql.bind_variable(v_cursor, rec_param_value.sql_parameter, v_program_id);

                elsif rec_param_value.parameter_value = 'IMP_RUN_ID' then
                    dbms_sql.bind_variable(v_cursor, rec_param_value.sql_parameter, p_imp_run_id);

                elsif rec_param_value.parameter_value = 'ROW_NUM' then
                    dbms_sql.bind_array(v_cursor, rec_param_value.sql_parameter, v_row_nums_array);

                else
                    dbms_sql.bind_variable(v_cursor, rec_param_value.sql_parameter, rec_param_value.parameter_value);                       
                end if;

            elsif rec_param_value.col_num is not null then --bind column vals
                select data bulk collect into v_param_vals_array
                  from imp_run_grid_incr irg
                 where imp_run_id = p_imp_run_id and row_num > v_start_row
                   and col_num = rec_param_value.col_num
                 order by row_num;

                dbms_sql.bind_array(v_cursor, ':' || rec_param_value.sql_parameter, v_param_vals_array);
            end if;
        end loop;

        v_rows_processed := dbms_sql.execute(v_cursor);
        dbms_sql.close_cursor(v_cursor);

        --TODO remove with Imp-121196
        if c_is_compare then
            set_cnt_pk_find(p_imp_run_id, p_imp_entity_id, v_rows_processed);
        end if;
    exception
        when others then
            close_cursor(v_cursor);
            v_errmsg := dbms_utility.format_error_backtrace || dbms_utility.format_error_stack ;
            raise_application_error(-20000, v_errmsg);

    end fill_existing_entity_pks;    


    procedure errors_for_existing_entities(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type, 
        p_entity_name in imp_entity.entity_name%type)
    as
    begin
        insert into imp_run_error(imp_run_id, error_msg, sql_text, imp_error_type_id, row_num)
        select p_imp_run_id, 'No entity to update with "Update" Import action, Entity Name: ' || p_entity_name,
                        empty_clob(), pkg_imp_run.c_et_pk, row_num
          from (select distinct row_num
                  from imp_run_grid_incr
                 where imp_run_id = p_imp_run_id
                   and row_num > (select start_row
                                    from imp_run
                                   where imp_run_id = p_imp_run_id)
                 minus
                select row_num
                  from imp_run_entity_pk
                 where imp_run_id = p_imp_run_id
                   and imp_entity_id = p_imp_entity_id);                    
    end errors_for_existing_entities;


    procedure new_entities(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type)
    as
        v_irun pkg_imp_run.cur_imp_run%rowtype;
        v_rows_processed number default 0;
    begin
        open pkg_imp_run.cur_imp_run(p_imp_run_id);
        fetch pkg_imp_run.cur_imp_run into v_irun;
        close pkg_imp_run.cur_imp_run;

        for rec_pk in (select distinct row_num
                         from imp_run_grid_incr
                        where imp_run_id = p_imp_run_id
                          and row_num > (select start_row
                                           from imp_run
                                          where imp_run_id = p_imp_run_id)
                        minus
                       select row_num
                         from imp_run_entity_pk
                        where imp_run_id = p_imp_run_id
                          and imp_entity_id = p_imp_entity_id
                        order by row_num) loop

            v_rows_processed := v_rows_processed + 1;
            create_entity(p_imp_run_id, p_imp_entity_id, p_xitor_type_id, rec_pk.row_num, v_irun);
        end loop;

        set_cnt_pk_new(p_imp_run_id, p_imp_entity_id, v_rows_processed); --TODO remove with Imp-121196
    end new_entities;


    procedure handle_insert_only(
        p_imp_run_id in imp_run.imp_run_id%type, 
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type)
    as
    begin
        new_entities(p_imp_run_id, p_imp_entity_id, p_xitor_type_id); 

        if not c_is_compare then --TODO remove with Imp-121196
            insert into imp_run_error(imp_run_id, error_msg, imp_error_type_id, row_num)
                 select p_imp_run_id, pk, pkg_imp_run.c_et_xitor_exists, row_num
                   from imp_run_entity_pk 
                  where imp_run_id = p_imp_run_id
                    and pk is not null and is_inserted = 0;
        end if;                    

        --delete existing entities
        delete from imp_run_entity_pk
         where imp_run_id = p_imp_run_id
           and imp_entity_id = p_imp_entity_id
           and pk is not null and is_inserted = 0;
    end handle_insert_only;


    procedure create_entity(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type,
        p_xitor_type_id in imp_entity.xitor_type_id%type,
        p_row_num in imp_run_grid_incr.row_num%type,
        p_irun in pkg_imp_run.cur_imp_run%rowtype)
    as
        v_pksql  clob;
        v_pksql2 clob;
        v_value  number;
    begin
        v_pksql := pkg_imp_run.build_insert_ent_sql(p_xitor_type_id, p_imp_entity_id, p_row_num, p_irun);
        v_pksql2 := replace(v_pksql, chr(13), '');
        v_pksql2 := replace(v_pksql2, chr(10), ' ');

        if not c_is_compare then --TODO remove with Imp-121196 
            v_value := pkg_imp_run.run_sql_ret(
                p_rid => p_imp_run_id, 
                p_sql => v_pksql2, 
                p_err_type_id => pkg_imp_run.c_et_new_xitor, 
                p_row_num => p_row_num);

            if v_value is not null then
                pkg_imp_run.add_pk(p_imp_run_id, p_imp_entity_id, p_row_num, v_value, 1);
            end if; 
        else
            pkg_imp_run.add_pk(p_imp_run_id, p_imp_entity_id, p_row_num, null, 1);
        end if;
    end create_entity;


    --TODO remove with Imp-121196
    function is_compare return boolean as 
        v_value param_system.value%type;
        v_ret boolean;
    begin
        select value into v_value
          from param_system where name='CompareFImpEntitySearch';

        if v_value = 1 then
            v_ret := true;
        else
            v_ret := false;
        end if;

        return v_ret;
    exception 
        when others then
            return false;        
    end is_compare;


    --TODO remove with Imp-121196
    procedure prepare_to_compare_data(p_imp_run in imp_run.imp_run_id%type) as
    begin
        if c_is_compare then
            --copy data to fimp_run_entity_pk for next compare
            insert into fimp_run_entity_pk
                (imp_run_entity_pk_id, imp_run_id, imp_entity_id, row_num, pk, is_inserted)
            select imp_run_entity_pk_id, imp_run_id, imp_entity_id, row_num, pk, is_inserted
              from imp_run_entity_pk
             where imp_run_id = p_imp_run;

             --commit for next autonomous drop_pks
             commit;
             --drop data from imp_run_entity_pk for next standard import 
             pkg_imp_run.drop_pks(p_imp_run);
        end if;    
    end prepare_to_compare_data;


    --TODO remove with Imp-121196
    procedure save_diff_data(
        p_imp_run_id in imp_run.imp_run_id%type,
        p_imp_entity_id in imp_entity.imp_entity_id%type) as
    begin
        --copy diff data to fimp_run_entity_pk_diff
        insert into fimp_run_entity_pk_diff
            (imp_run_id, old_imp_entity_id, old_row_num, old_pk, old_is_inserted,
             new_imp_entity_id, new_row_num, new_pk, new_is_inserted)
        select p_imp_run_id, old_imp_entity_id, old_row_num, old_pk, old_is_inserted,
               new_imp_entity_id, new_row_num, new_pk, new_is_inserted
          from (select * from
                   (select imp_entity_id as old_imp_entity_id, row_num as old_row_num, pk as old_pk, is_inserted as old_is_inserted
                      from imp_run_entity_pk
                     where imp_run_id = p_imp_run_id
                       and imp_entity_id = p_imp_entity_id) a
                  full outer join
                   (select imp_entity_id as new_imp_entity_id, row_num as new_row_num, pk as new_pk, is_inserted as new_is_inserted
                      from fimp_run_entity_pk
                     where imp_run_id = p_imp_run_id
                       and imp_entity_id = p_imp_entity_id) b
                  on old_imp_entity_id = new_imp_entity_id 
                 and old_row_num = new_row_num and old_is_inserted = new_is_inserted) a
         where old_row_num is null or new_row_num is null;
    end save_diff_data;

end pkg_fimp_run;
/