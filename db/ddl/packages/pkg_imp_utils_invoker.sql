CREATE OR REPLACE PACKAGE BODY PKG_IMP_UTILS_INVOKER 
/*
 * Copyright 2003-2020 OneVizion, Inc. All rights reserved.
 */
as
    procedure interrupt_import(proc_id in process.process_id%type, p_job_creator in varchar2) 
    is
        v_old_status process_status.status%type;
        v_comments process.comments%type;
        v_job_name varchar2(100);
        v_job_state varchar2(100);
        v_proces_status_id process_status.process_status_id%type;

        e_no_job exception;
        pragma exception_init(e_no_job, -27475);
    begin
        select status, process_status_id into v_old_status, v_proces_status_id
        from process p join process_status ps on (ps.process_status_id = p.status_id)
        where p.process_id = proc_id;

        v_comments := pkg_label.get_label_system(5776) || v_old_status || pkg_label.get_label_system(5777);

        begin
            if v_proces_status_id = 11 then
                v_job_name := 'IMP_RECOVERY_' || proc_id;
            else  
                v_job_name := 'IMP_' || proc_id;
            end if;            

            select state into v_job_state 
              from all_scheduler_jobs
             where job_name = v_job_name 
               and upper(owner) = upper(p_job_creator);

            v_job_name := p_job_creator || '.' || v_job_name;

            if (v_job_state = 'SCHEDULED') then
                dbms_scheduler.drop_job(v_job_name);
            else
                dbms_scheduler.stop_job(v_job_name);
            end if;
        exception    
            when e_no_job then 
                null;
            when no_data_found then 
                null;
        end; 

        update process
        set status_id = 14,
            end_date = current_date,
            scheduler_end = current_date,
            runtime = round((current_date - scheduler_start) * 86400),
            comments = comments || chr(13) || chr(10) || v_comments
        where process_id = proc_id;

        delete from process_run where process_id = proc_id;
    end interrupt_import;
end pkg_imp_utils_invoker;
/