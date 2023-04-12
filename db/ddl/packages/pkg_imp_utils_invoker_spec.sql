CREATE OR REPLACE PACKAGE PKG_IMP_UTILS_INVOKER 
/*
 * Copyright 2003-2020 OneVizion, Inc. All rights reserved.
 */
    authid current_user
    /**
     * Import related routines to execute under invoker's permissions. Usually 
     * import jobs are started under _user schema and when trying to interrupt
     * import from GUI, PL/SQL procedures will be executed under owner schema,
     * which can't see _users's jobs
     */
as
    /**
     * Stop oracle job and update import/recovery import status to "Interrupted".
     * Add comment about import/recovery import status before interruption
     * @param proc_id
     * @param p_job_creator oracle user who started job
     */
    procedure interrupt_import(proc_id in process.process_id%type, p_job_creator in varchar2);

end pkg_imp_utils_invoker;
/