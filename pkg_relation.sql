CREATE OR REPLACE PACKAGE BODY PKG_RELATION as
    procedure no_direct_table_mods as
    begin
      if not allowtablemods then
        raise_application_error (-20000, 'Do not modify the table directly.  Use pkg_relation instead.');
      end if;
    end no_direct_table_mods;

    /**
     * Check if relations are locked for specified xitor and relation type.
     * If so raise exception
     */
    procedure check_relation_locks(
      p_rtid in relation_type.relation_type_id%type,
      p_cid in relation.child_id%type);

    procedure unassign_fields(p_prim_ttid number, p_parent_ttid number, p_unassign_tid number default 1);

    procedure new_relation(pid number, cid number, rtid number)
    as
        retval varchar2(1000);
      strRel varchar2(1000);
      p varchar2(200);
      c varchar2(200);
      c_xtid number;
      ClassChildRelCnt number;
         c_key varchar2(1000);
      rel_cnt number;
    begin
        if pid is null or cid is null then
           select p.xitor_type, c.xitor_type into p, c from relation_type t, xitor_type p, xitor_type c
            where t.relation_type_id=rtid
             and t.parent_type_id=p.xitor_type_id and t.child_type_id=c.xitor_type_id;
           strRel := p||'-->'||c;
           if pid is null then
              strRel := strRel || ', '||p||' is Null';
           else
              strRel := strRel || ', '||c||' is Null';
           end if;
           raise_application_error(-20000, '<ERRORMSG>ParentID/ChildID cannot be Null! ('||strRel||')</ERRORMSG>');
        end if;

        begin
          select child_type_id into c_xtid
          from
            relation_type rt, xitor p, xitor c
          where
            p.xitor_id = pid
            and c.xitor_id = cid
            and rt.parent_type_id = p.xitor_type_id
            and rt.child_type_id = c.xitor_type_id
            and rt.relation_type_id = rtid;
        exception
          when no_data_found then
            begin
              raise_application_error(-20000, '<ERRORMSG>TrackorTypeIDs of Parent (pid='||pid||') and Child (cid='||cid||') do not belong to RelationTypeId ('||rtid||')!</ERRORMSG>');
            end;
        end;

        check_relation_locks(rtid, cid);

        --select child_type_id into c_xtid from relation_type where relation_type_id=rtid;

        --Check Parent Class-->Child restriction
        ClassChildRelCnt := 1;
        for rec in (select x.*, xt.xitor_type from xitor x, v_xitor_class v, xitor_type xt
            where x.xitor_id=pid and v.xitor_type_id=x.xitor_type_id
            and x.xitor_type_id = xt.xitor_type_id
            and (v.program_id=x.program_id or v.program_id is null) and rownum=1)
        loop
            select count(*) into ClassChildRelCnt from children_xitor_class_xref cx
              where cx.xitor_class_id=rec.xitor_class_id and cx.xitor_type_id=c_xtid;
            if ClassChildRelCnt = 0 then
               select xitor_key into c_key from xitor where xitor_id=cid;

               raise_application_error(-20000, '<ERRORMSG>Cannot assign Child (Trackor ID: '||cid||'; Key="'||c_key||'") to Parent (Trackor ID: '|| pid ||'; Key="'||rec.xitor_key||'"; Class="'||rec.class_name||'"; Trackor Type="' || rec.xitor_type || '") due to ParentClass to child Trackors restriction.</ERRORMSG>');
            end if;
        end loop;


        retval := null;
        select count(*) into rel_cnt from relation 
          where child_id=cid and parent_id=pid and relation_type_id=rtid;

        if rel_cnt = 0 then  
           allowtablemods := true;
           insert into relation (child_id, parent_id, relation_type_id)
           values (cid, pid, rtid);

           retval := pkg_ruleator.execute_trigger(21, rtid, pid, cid);
           if retval is not null then
              raise_application_error(-20000, retval);
           end if;

           --Add relations with parents into ancestor
           allowtablemods := true;
           merge into ancestor ts
             using  (
               select distinct t.parent_id p, cid c, rt.parent_type_id, c_xtid child_type_id
               from relation t, relation_type rt where t.relation_type_id=rt.relation_type_id(+)
               start with t.child_id=cid
               connect by prior t.parent_id = t.child_id
           )s
           on (s.c=ts.child_id and s.p=ts.parent_id)
           when not matched then
           insert (ts.parent_id, ts.child_id, ts.p_xitor_type_id, ts.c_xitor_type_id)
             values (s.p, s.c, s.parent_type_id, s.child_type_id);


           --Add missed relations for all children underneath cid
           allowtablemods := true;
           for rec in (select distinct t.child_id c, rt.child_type_id  from relation t, relation_type rt
               where t.relation_type_id=rt.relation_type_id(+)
               start with t.parent_id=cid
               connect by prior t.child_id = t.parent_id)
           loop
             merge into ancestor ts
              using  (
               select distinct t.parent_id p, rec.c c, rt.parent_type_id, rec.child_type_id child_type_id
               from relation t, relation_type rt
               where t.relation_type_id=rt.relation_type_id(+)
               start with t.child_id=rec.c
               connect by prior t.parent_id = t.child_id
             )s
             on (s.c=ts.child_id and s.p=ts.parent_id)
             when not matched then
             insert (ts.parent_id, ts.child_id, ts.p_xitor_type_id, ts.c_xitor_type_id)
               values (s.p, s.c, s.parent_type_id, s.child_type_id);
           end loop;


           check_relation_uniqueness(cid);
           pkg_wp.Add_Task_For_Relation(cid, pid);

           --Create initial log for the task added
           for rec in (select wp_workplan_id from wp_workplan where xitor_id=pid) loop
             for rec2 in (SELECT * FROM WP_TASKS WHERE WP_WORKPLAN_ID=rec.wp_workplan_id and XITOR_ID=CID)
             loop
                pkg_audit.log_task_changes_full(rec2, rec2, Null, pkg_audit.c_la_insert);
             end loop;
           end loop;
        end if;
        allowtablemods := false;
    end new_relation;


    procedure new_relation_plus(oldpid number, pid number, cid number, rtid number, prgid number)
    as
        ptid number;
        ctid number;
    begin
        new_relation(pid, cid, rtid);

        /*
        Recreate relations with children in this case:
        County     -->SAR    -->Cand
             |         |          |
             |->EFile  |->EFile   |->EFile

        If we are changing County for a SAR then we need to change County for all
        EFiles underneath this SAR
        */


        select rt.parent_type_id, rt.child_type_id into ptid, ctid
           from relation_type rt where rt.relation_type_id = rtid;

        for rec in (
            select distinct r.relation_type_id, child_type_id
            from relation_type r
            where r.parent_type_id=ptid and r.cardinality_id=2
                and r.child_type_id<>ctid
                and exists (
                    select 1 from (
                        select xt.xitor_type_id
                        from
                            relation_type t
                            join xitor_type xt on (t.child_type_id = xt.xitor_type_id)
                        where xt.is_static_definition=0
                            and xt.program_id = prgid
                        start with t.parent_type_id=ctid
                        connect by prior t.child_type_id = t.parent_type_id
                    ) where xitor_type_id = r.child_type_id
                )
        ) loop
            for rec_chld in (
                select child_id
                from relation rel 
                where 
                    rel.relation_type_id=rec.relation_type_id
                    and rel.parent_id=oldpid
                    and rel.child_id in (
                        select child_id
                        from relation r
                            join relation_type rt2 on (rt2.relation_type_id = r.relation_type_id)
                        where rt2.child_type_id = rec.child_type_id
                        start with r.parent_id = cid
                        connect by prior r.child_id = r.parent_id
                    )
            ) loop
             --We change County for EFile only if EFile is attached to SAR or Cand.
             --If EFile attached only to County then we need nothing to do.
             del_relation(oldpid, rec_chld.child_id, rec.relation_type_id);
             new_relation(pid, rec_chld.child_id, rec.relation_type_id);
            end loop;
        end loop;

    end new_relation_plus;


    procedure del_relation(pid number, cid number, rtid number) as
    begin
      check_relation_locks(rtid, cid); -- will rise exception if relation is locked
      del_relation_with_locks(pid, cid, rtid);
    end del_relation;

    procedure log_relation_deletion(pid number, cid number, rtid number) is
        v_pkey varchar2(4000 char);
        v_ckey varchar2(4000 char);
        v_child_ttid xitor_type.xitor_type_id%type;
    begin 
        select pkg_config_field_rpt.getValStrByStaticID(x.xitor_id, f.config_field_id) 
          into v_pkey
          from xitor x, config_field f
         where x.xitor_id = pid 
           and f.xitor_type_id = x.xitor_type_id 
           and f.is_static = 1 and f.config_field_name = 'XITOR_KEY';

        select pkg_config_field_rpt.getValStrByStaticID(x.xitor_id, f.config_field_id), x.xitor_type_id 
          into v_ckey, v_child_ttid
          from xitor x, config_field f
         where x.xitor_id = cid 
           and f.xitor_type_id = x.xitor_type_id 
           and f.is_static = 1 and f.config_field_name = 'XITOR_KEY';

        pkg_audit.log_changes('RELATION',            -- tablename
                              'PARENT_ID',           -- field
                              cid,         -- pkey
                              'D',                   -- action
                              pkg_sec.get_cu(),      -- user_id
                              pid,        -- from_number
                              Null,                  -- to_number
                              v_pkey||'->'||v_ckey,      -- from_char
                              Null,                  -- to_char
                              Null,                  -- from_date
                              Null,                  -- to_date
                              Null,                  -- from_blob_data_id
                              Null,                  -- to_blob_data_id
                              rtid, -- linenumber
                              'r_'||to_char(v_child_ttid)||'_'||to_char(rtid),
                              null,
                              null);
    end log_relation_deletion;

    procedure del_relation_with_locks(pid number, cid number, rtid number) as
        retval varchar2(255);
        strRel varchar2(1000);
        p varchar2(200);
        c varchar2(200);
    begin
        if pid is null or cid is null then
           select p.xitor_type, c.xitor_type into p, c from relation_type t, xitor_type p, xitor_type c
           where t.relation_type_id = rtid
             and t.parent_type_id=p.xitor_type_id and t.child_type_id=c.xitor_type_id;
           strRel := p||'-->'||c;
           if pid is null then
              strRel := strRel || ', '||p||' is Null';
           else
              strRel := strRel || ', '||c||' is Null';
           end if;
           raise_application_error(-20000, '<ERRORMSG>ParentID/ChildID cannot be Null! ('||strRel||')</ERRORMSG>');
        end if;

        --need to log relation deletion before rule calls to properly fill audit_call_stack_temp
        log_relation_deletion(pid, cid, rtid);

        retval := pkg_ruleator.execute_trigger(23, rtid, pid, cid);
        if retval is not null then
           raise_application_error(-20000, retval);
        end if;

        -- delete direct relation if there are
        -- no indirect relations between pid and cid exists
        allowtablemods := true;
        delete from ancestor
        where parent_id = pid and child_id = cid
        and not exists (
            select 1 from relation
             where parent_id = pid and child_id <> cid
            start with child_id = cid connect by prior parent_id = child_id
        );

        if (sql%rowcount <> 0) then
            -- if direct relation were deleted in prev statement
            -- delete relations between cid and all parents
            allowtablemods := true;
            delete from ancestor
            where child_id = cid and paths_wout_xid(parent_id, cid, pid) = 0;
        end if;

        -- delete indirect relations between pid and children of cid
        -- if there are no other relations exists
        allowtablemods := true;
        delete from ancestor
        where parent_id = pid  and child_id in (
            select a.child_id from ancestor a
            where a.parent_id = cid
            and paths_wout_xid(pid, a.child_id, cid) = 0);

        -- loop through all pid's parents with which cid have only relation via pid
        for rec_parent in (
                select pr.parent_id, pr.child_id from relation pr
                where paths_wout_xid(pr.parent_id, cid, pid) = 0
                start with pr.child_id = pid connect by prior pr.parent_id = pr.child_id) loop

            -- delete indirect relations between parents of pid and children of cid
            -- making sure relation is only possible via cid
            allowtablemods := true;
            delete from ancestor a1
            where a1.parent_id = rec_parent.parent_id and a1.child_id in (
                select a.child_id from ancestor a
                where a.parent_id = cid
                and paths_wout_xid(rec_parent.parent_id, a.child_id, cid) = 0);
            null;
        end loop;

        -- need to delete form relation after ancestor cleanup,
        -- because relying on this data to check relation paths
        allowtablemods := true;
        delete from relation where parent_id = pid
           and child_id = cid and relation_type_id = rtid;

        pkg_wp.Delete_tasks_for_relation(cid, pid);

        --delete records from blob_ancestor
        delete from blob_ancestor b 
              where b.blob_id in (select b.blob_id
                                    from blob_ancestor b
                                   where b.blob_owner_id = cid)
                and b.blob_owner_id = pid;

        delete from blob_ancestor b 
              where b.blob_id in (select b.blob_id 
                                    from blob_ancestor b 
                                   where b.blob_owner_id = cid) 
                and b.blob_owner_id in (select a.parent_id 
                                          from ancestor a 
                                         where a.child_id = pid
                                           and a.parent_id not in(select a2.parent_id 
                                                                    from ancestor a2 
                                                                   where a2.parent_id = a.parent_id and a2.child_id = cid));

        allowtablemods := false;
    end del_relation_with_locks;


    function new_relation_type(
        pid             number,
        cid             number,
        cardinality     number,
        childreqparent  number,
        onDeleteCascade number,
        uniqueBy        number  default Null)
        return relation_type.relation_type_id%type as

        rtid number;
        parent_pid number := null;
        child_pid number := null;
    begin
        if pid is not null then
            select program_id into parent_pid from xitor_type where xitor_type_id = pid;
        end if;
        select program_id into child_pid from xitor_type where xitor_type_id = cid;

        if parent_pid is not null and parent_pid <> child_pid then
            raise_application_error(-20000,'<ERRORMSG>Parental and child Trackor Types are assigned to different programs</ERRORMSG>');
        end if;

        allowtablemods := true;
        insert into relation_type
                    (child_type_id, parent_type_id, cardinality_id,
                     child_requires_parent, on_parent_delete_cascade, unique_by_xt_id, program_id)
             values (cid, pid, cardinality,
                     childreqparent, onDeleteCascade, uniqueBy, child_pid)
          returning relation_type_id
               into rtid;

        --Assign child to all classes of the parent
        insert into children_xitor_class_xref (xitor_class_id, xitor_type_id, program_id)
        select a.xitor_class_id, cid, child_pid
          from v_xitor_class a
         where a.xitor_type_id = pid;


        --recreate ancestor_type table
        allowtablemods := true;
        recreate_ancestor_type(child_pid);

        allowtablemods := false;
        return rtid;
    end new_relation_type;


    procedure del_relation_type_all(xid number)
    as
        v_pid xitor_type.program_id%type;
    begin
        select program_id into v_pid from xitor_type where xitor_type_id = xid;
        pkg_sec_role.allowtablemods := true;

        Delete from xitor_count where PARENT_CHILD_XT_ID=xid;
        Delete from xitor_count_user where PARENT_CHILD_XT_ID=xid;

        --Drop all relations when we are deleting a XitorType
        for rec in (select     t.relation_type_id, t.parent_type_id,
                               t.child_type_id
                          from relation_type t, xitor_type m
                         where m.xitor_type_id = t.child_type_id
                    start with t.parent_type_id = xid
                    connect by prior m.xitor_type_id = t.parent_type_id) loop

            for sec_group_rec in (select sec_group_program_id from sec_group_program
                where relation_type_id= rec.relation_type_id) loop
                pkg_sec_priv_program.delete_sec_group(sec_group_rec.sec_group_program_id);
            end loop;

            pkg_sec_role.allowtablemods := true;
            allowtablemods := true;

            delete from relation_type
                  where parent_type_id = rec.parent_type_id
                    and child_type_id = rec.child_type_id;

            delete from ancestor_type
                  where parent_type_id = rec.parent_type_id
                    and child_type_id = rec.child_type_id;

        end loop;

        for rec in (select parent_type_id, child_type_id, relation_type_id
                      from relation_type
                     where child_type_id = xid) loop
            for sec_group_rec in (select sec_group_program_id from sec_group_program
                where relation_type_id= rec.relation_type_id) loop
                pkg_sec_priv_program.delete_sec_group(sec_group_rec.sec_group_program_id);
            end loop;

            pkg_sec_role.allowtablemods := true;
            allowtablemods := true;
            delete from relation_type
                  where parent_type_id = rec.parent_type_id
                    and child_type_id = rec.child_type_id;
        end loop;

        --recreate ancestor_type table
        if not pkg_xitor_type.is_deleting then
          allowtablemods := true;
          recreate_ancestor_type(v_pid);
          allowtablemods := false;
        end if;
        pkg_sec_role.allowtablemods := false;

    end del_relation_type_all;


    procedure del_relation_type(rtid number)
    as
        v_rel_type_row relation_type%rowtype;
    begin
        unassign_parent_tabs(rtid);
        unassign_parent_fields(rtid, 1);

        pkg_sec_role.allowtablemods:=true;
        allowtablemods := true;
        select * into v_rel_type_row 
          from relation_type
         where relation_type_id = rtid;

        --Unassign child from all classes of the parent
        delete from children_xitor_class_xref
         where xitor_class_id in (select xitor_class_id from v_xitor_class where xitor_type_id = v_rel_type_row.parent_type_id)
           and xitor_type_id = v_rel_type_row.child_type_id;


        --Delete all relations between Parent and Child in question
        for rec_rel in (select parent_id, child_id, relation_type_id from relation where relation_type_id = v_rel_type_row.relation_type_id) loop
            del_relation_with_locks(rec_rel.parent_id, rec_rel.child_id, rec_rel.relation_type_id);
        end loop;

        --Delete security groups
        for rec_sec in (select sec_group_program_id from sec_group_program where relation_type_id = v_rel_type_row.relation_type_id) loop
            pkg_sec_priv_program.delete_sec_group(rec_sec.sec_group_program_id);
        end loop;

        --Delete this relation type
        allowtablemods := true;
        delete from relation_type
         where relation_type_id = v_rel_type_row.relation_type_id;

        --recreate ancestor_type table
        allowtablemods := true;
        recreate_ancestor_type(v_rel_type_row.program_id);

        pkg_sec_role.allowtablemods:=false;
        allowtablemods := false;
    end del_relation_type;


    --TODO replace with view or query in asp
    procedure getparentsbycardinality(
        pid         number,
        xtid        number,
        list in out varchar2)
    as
    begin
        for rec in (select distinct xt.xitor_type_id, xt.xitor_type,
                                    t.child_type_id, t.cardinality_id
                               from relation_type t
                               join xitor_type xt on (t.parent_type_id = xt.xitor_type_id)
                              where xt.is_static_definition = 0
                                and t.child_type_id = xtid
                                and t.cardinality_id <> 3) loop
            if instr(',' || list, ',' || rec.xitor_type_id || ',') = 0 then
                list := list || rec.xitor_type_id || ',';
            end if;

            getparentsbycardinality(pid, rec.xitor_type_id, list);
        end loop;
    end getparentsbycardinality;

    /*getParentsByFromIDandCardinality in ASP*/
    function getParentsByIdAndCardinality(
        prgid  in       number,
        xtid   in     number
        ) return varchar2
    as
    list varchar2(4000);
    begin
        --list := pkg_vqutils.getGlobalTemp('getParentsByIdAndCardinality', prgid, xtid);
        --if list is null then
           getparentsbycardinality(prgid, xtid, list);
           list := to_char(xtid) || ',' || list;
           if substr(list, length(list), 1) = ',' then
              list := substr(list, 1, length(list)-1);
           end if;

           --pkg_vqutils.setGlobalTemp('getParentsByIdAndCardinality', prgid, xtid, list);
        --end if;
        return list;
    end getParentsByIdAndCardinality;

    procedure unassign_parent_tabs(rtid relation_type.relation_type_id%type) as
        v_relation_type_id relation_type.relation_type_id%type;
        v_child_type_id relation_type.child_type_id%type;
    begin
        select relation_type_id, child_type_id into v_relation_type_id, v_child_type_id
          from relation_type
         where relation_type_id = rtid;

        --Unassign parental Tabs from all Config Apps
        --if there is no any connection through other branches
        for rec_parent in (
            select distinct z.parent_type_id from (
                select
                    t.relation_type_id,
                    t.parent_type_id,
                    t.child_type_id,
                    t.cardinality_id,
                    tp.relation_type_id as prtid
                from
                    relation_type t
                    left outer join relation_type tp on (tp.child_type_id = t.parent_type_id)
                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
                where
                    xt.is_static_definition = 0
            ) z
            start with z.relation_type_id = v_relation_type_id
            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3 and z.prtid is not null
            minus
            select distinct z.parent_type_id from (
                select
                    t.relation_type_id,
                    t.parent_type_id,
                    t.child_type_id,
                    t.cardinality_id
                from
                    relation_type t
                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
                where
                    xt.is_static_definition = 0
            ) z
            start with z.child_type_id = v_child_type_id and z.relation_type_id <> v_relation_type_id
            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
        ) loop
            delete from config_app_group_xref
            where config_app_group_xref_id in(
                select
                    cagx.config_app_group_xref_id
                from
                    config_group cg
                    join config_app_group_xref cagx on (cagx.config_group_id = cg.config_group_id)
                    join config_app ca on (ca.config_app_id = cagx.config_app_id)
                where
                    cg.xitor_type_id = rec_parent.parent_type_id
                    and ca.xitor_type_id = v_child_type_id
                    and ((ca.is_master_app = 0) or (ca.is_master_app = 1 and cg.is_master_tab <> 1))
            );
        end loop;
    end unassign_parent_tabs;

    procedure unassign_parent_fields(rtid relation_type.relation_type_id%type, p_unassign_tid number default 1) as
        v_relation_type_id relation_type.relation_type_id%type;
        v_child_type_id relation_type.child_type_id%type;
    begin
        select relation_type_id, child_type_id into v_relation_type_id, v_child_type_id
          from relation_type
         where relation_type_id = rtid;

        --Unassign parental Config Fields from all Config Tabs
        --if there is no any connection through other branches
        for rec_parent in (
            select distinct z.parent_type_id from (
                select
                    t.relation_type_id,
                    t.parent_type_id,
                    t.child_type_id,
                    t.cardinality_id,
                    tp.relation_type_id as prtid
                from
                    relation_type t
                    left outer join relation_type tp on (tp.child_type_id = t.parent_type_id)
                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
                where
                    xt.is_static_definition = 0
            ) z
            start with z.relation_type_id = v_relation_type_id
            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3 and z.prtid is not null
            minus
            select distinct z.parent_type_id from (
                select
                    t.relation_type_id,
                    t.parent_type_id,
                    t.child_type_id,
                    t.cardinality_id
                from
                    relation_type t
                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
                where
                    xt.is_static_definition = 0
            ) z
            start with z.child_type_id = v_child_type_id and z.relation_type_id <> v_relation_type_id
            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
        ) loop
            unassign_fields(v_child_type_id, rec_parent.parent_type_id, p_unassign_tid);
        end loop;
    end unassign_parent_fields;

--    procedure unassign_parent_tabs_fields(rtid number)
--    as
--        v_rel_type_row relation_type%rowtype;
--    begin
--        select * into v_rel_type_row from relation_type
--        where relation_type_id = rtid;
--
--        --Unassign parental Tabs from all Config Apps
--        --if there is no any connection through other branches
--        for rec_parent in (
--            select distinct z.parent_type_id from (
--                select
--                    t.relation_type_id,
--                    t.parent_type_id,
--                    t.child_type_id,
--                    t.cardinality_id
--                from
--                    relation_type t
--                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
--                where
--                    xt.is_static_definition = 0
--            ) z
--            start with z.relation_type_id = v_rel_type_row.relation_type_id
--            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
--            minus
--            select distinct z.parent_type_id from (
--                select
--                    t.relation_type_id,
--                    t.parent_type_id,
--                    t.child_type_id,
--                    t.cardinality_id
--                from
--                    relation_type t
--                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
--                where
--                    xt.is_static_definition = 0
--            ) z
--            start with z.child_type_id = v_rel_type_row.child_type_id and z.relation_type_id <> v_rel_type_row.relation_type_id
--            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
--        ) loop
--            delete from config_app_group_xref
--            where config_app_group_xref_id in(
--                select
--                    cagx.config_app_group_xref_id
--                from
--                    config_group cg
--                    join config_app_group_xref cagx on (cagx.config_group_id = cg.config_group_id)
--                    join config_app ca on (ca.config_app_id = cagx.config_app_id)
--                where
--                    cg.xitor_type_id = rec_parent.parent_type_id
--                    and ((ca.is_master_app = 0) or (ca.is_master_app = 1 and cg.is_master_tab <> 1))
--            );
--        end loop;
--
--        --Unassign parental Config Fields from all Config Tabs
--        --if there is no any connection through other branches
--        for rec_parent in (
--            select distinct z.parent_type_id from (
--                select
--                    t.relation_type_id,
--                    t.parent_type_id,
--                    t.child_type_id,
--                    t.cardinality_id
--                from
--                    relation_type t
--                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
--                where
--                    xt.is_static_definition = 0
--            ) z
--            start with z.relation_type_id = v_rel_type_row.relation_type_id
--            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
--            minus
--            select distinct z.parent_type_id from (
--                select
--                    t.relation_type_id,
--                    t.parent_type_id,
--                    t.child_type_id,
--                    t.cardinality_id
--                from
--                    relation_type t
--                    join xitor_type xt on (xt.xitor_type_id = t.parent_type_id)
--                where
--                    xt.is_static_definition = 0
--            ) z
--            start with z.child_type_id = v_rel_type_row.child_type_id and z.relation_type_id <> v_rel_type_row.relation_type_id
--            connect by prior z.parent_type_id = z.child_type_id and prior z.cardinality_id <> 3
--        ) loop
--            unassign_parent_fields(v_rel_type_row.child_type_id, rec_parent.parent_type_id, 1);
--        end loop;
--    end unassign_parent_tabs_fields;


    procedure check_relation_uniqueness(p_child_id in relation.child_id%type)
    as
        uniqueByXtID number;
        childTypeID number;
        childPID number;
        childXitorKey varchar2(2000);
        parentXTName label_program.label_program_text%type;
        lbl1 label_program.label_program_text%type;
        lbl2 label_program.label_program_text%type;
        lblChildKey label_program.label_program_text%type;

        cursor cur_parents(p_child_id in relation.child_id%type) is
            select a.parent_id, t.relation_type_id
              from ancestor a, xitor x, relation_type t, xitor x2
             where a.child_id = p_child_id  and a.parent_id = x.xitor_id
               and x.xitor_type_id = t.unique_by_xt_id
               and t.child_type_id=x2.xitor_type_id and x2.xitor_id=p_child_id
            union
            select null, t.relation_type_id
              from relation_type t, xitor x
             where t.unique_by_xt_id = x.xitor_type_id
               and t.unique_by_xt_id = t.child_type_id and x.xitor_id = p_child_id;

        cursor cur_uniq_by(p_rtid in relation_type.relation_type_id%type) is
            select unique_by_xt_id, l.label_text
              from relation_type t, xitor_type xt, vw_label l
             where t.relation_type_id = p_rtid
               and xt.xitor_type_id = t.unique_by_xt_id
               and l.label_id = xt.applet_label_id
               and l.app_lang_id = pkg_sec.get_lang();
    begin
        for rec_parents in cur_parents(p_child_id) loop
            for rec_uniq_by in cur_uniq_by(rec_parents.relation_type_id) loop
                uniqueByXtID := rec_uniq_by.Unique_By_Xt_Id;
                parentXTName := rec_uniq_by.Label_Text;

                if Nvl(uniqueByXtID, -1) <> -1 then
                    select x.xitor_type_id, x.xitor_key, l.label_text, x.program_id
                      into childTypeID, childXitorKey, lblChildKey, childPID
                      from xitor x, xitor_type xt, vw_label l
                     where x.xitor_id = p_child_id and xt.xitor_type_id=x.xitor_type_id
                       and l.label_id = xt.xitorid_label_id and l.app_lang_id=pkg_sec.get_lang();

                    if uniqueByXtID = childTypeID then
                        --The Xitor Key should be unique across itself within current Z/P
                        for rec in (select 1 from dual where exists (select c.xitor_id from
                            xitor c where c.xitor_type_id=childTypeID and (c.program_id=childPID or c.program_id is Null)
                            and upper(c.xitor_key) = upper(childXitorKey) and c.xitor_id<>p_child_id))
                        loop
                            lbl1 := pkg_label.get_label_system(2848, pkg_sec.get_lang()); --'is not unique'
                            raise_application_error(-20000, '<ERRORMSG>'||lblChildKey || ' "'||childXitorKey||'" '|| lbl1 ||'!</ERRORMSG>');
                        end loop;
                    else
                        --The Xitor Key should be unique across a specific Parent from some level above
                        for rec in (
                            select p.xitor_key 
                              from ancestor a,
                                  (select xitor_key, xitor_id from xitor p where p.xitor_id = rec_parents.parent_id) p,
                                  (select xitor_key, xitor_id from xitor c where c.xitor_type_id=childTypeID and (c.program_id=childPID or c.program_id is Null)) c
                             where a.child_id=c.xitor_id and a.parent_id=p.xitor_id and c.xitor_id<>p_child_id and
                             upper(c.xitor_key) = upper(childXitorKey)) loop

                            lbl1 := pkg_label.get_label_system(2848, pkg_sec.get_lang()); --'is not unique'
                            lbl2 := pkg_label.get_label_system(2849, pkg_sec.get_lang()); --'is not unique'
                            raise_application_error(-20000, '<ERRORMSG>'||lblChildKey || ' "'||childXitorKey||'" '|| lbl1 ||' ' || lbl2 || ' ' ||parentXTName|| ' "'||rec.xitor_key||'"!</ERRORMSG>');
                        end loop;
                     end if;
                end if;
            end loop; --cur_uniq_by
        end loop; --cur_parents
    end check_relation_uniqueness;

    procedure getChildrenToBeDeleted(
        pid         number,
        xtid        number,
        list in out varchar2)
    as
    begin
        for rec in (
            select t.child_type_id
            from
                relation_type t
                join xitor_type xt on (t.child_type_id = xt.xitor_type_id)
            where xt.is_static_definition=0
                and xt.program_id=pid
                and t.parent_type_id=xtid
                and t.on_parent_delete_cascade=1
        ) loop
            list := list || rec.child_type_id || ',';
            getChildrenToBeDeleted(pid, rec.child_type_id, list);
        end loop;
    end getChildrenToBeDeleted;

    procedure check_relation_locks(
        p_rtid in relation_type.relation_type_id%type,
        p_cid in relation.child_id%type) as

        v_locks number;
    begin
        select count(*) into v_locks from relation_lock
        where xitor_id = p_cid and relation_type_id = p_rtid;

        if (v_locks > 0) then
            raise_application_error(-20000, '<ERRORMSG>Can''t change locked relation</ERRORMSG>');
        end if;
    end check_relation_locks;

    function get_parents_children_count(xitorID number, pcXitorTypeID number, progID number, isParent number) return number
    as
        primXitorTypeID number;
        showAllInContainer number;
        rtID number;
        cList varchar2(4000);
        v_cnt number;

        procedure update_xitor_count(
            xitorID in number,
            pcXitorTypeID in number,
            cnt in number)is
            pragma autonomous_transaction;

            e_integrity_constraint exception;
            pragma exception_init(e_integrity_constraint, -02291);
        begin
            merge into xitor_count t1
            using
              (select xitorID xitor_id, pcXitorTypeID parent_child_xt_id from dual) t2
            on(t1.xitor_id = t2.xitor_id and t1.parent_child_xt_id = t2.parent_child_xt_id)
            when matched then
               update set t1.count = cnt
            when not matched then
               insert (xitor_id, parent_child_xt_id, count)
               values (xitorID, pcXitorTypeID, cnt);

            commit;
        exception
            when dup_val_on_index then
                --another thread inserted data row just before us
                null;
            when e_integrity_constraint then
                --xitor deleted
                null;
        end update_xitor_count;

    begin
        begin
            select count into v_cnt from xitor_count t
             where t.xitor_id=xitorID and t.parent_child_xt_id=pcXitorTypeID
               and t.count is not null;
        exception
            when no_data_found then
                --count parents and update xitor_count
                null;
        end;

        if v_cnt is null then
            begin
                if isParent = 1 then --get number of parents
                    select count(distinct a.xitor_id) into v_cnt from ancestor_p a
                    where a.child_id = xitorID and a.xitor_type_id = pcXitorTypeID;

                else --get number of children
                    select x.xitor_type_id, rt.show_all_in_trcontainer, rt.relation_type_id into primXitorTypeID, showAllInContainer, rtID
                    from Xitor x, relation_type rt
                    where x.xitor_id=xitorID and rt.parent_type_id=x.xitor_type_id and rt.child_type_id=pcXitorTypeID;

                    if showAllInContainer = 1 then
                        select count(distinct t.child_id) into v_cnt
                        from relation t, xitor c
                        where t.parent_id = xitorID and t.relation_type_id=rtID
                        and t.child_id = c.xitor_id and c.xitor_type_id = pcXitorTypeID;
                    else
                       cList := pkg_relation.get_all_underneath_XTID(primXitorTypeID, progID);

                        select count(distinct t.child_id) into v_cnt
                        from relation t, xitor c
                        where t.parent_id = xitorID and t.relation_type_id=rtID
                        and t.child_id = c.xitor_id and c.xitor_type_id = pcXitorTypeID
                        and not exists (
                           select r.parent_id from relation r, relation_type rt
                           where r.child_id = t.child_id and rt.cardinality_id = 2
                           and r.relation_type_id = rt.relation_type_id
                           and rt.child_type_id = pcXitorTypeID and rt.parent_type_id <> primXitorTypeID
                           and rt.parent_type_id <> pcXitorTypeID
                           and rt.parent_type_id in (select * from the (select cast(pkg_str.split_str2num(cList) as tableOfNum ) from dual)) );
                    end if;
                end if;

                update_xitor_count(xitorID, pcXitorTypeID, v_cnt);
            exception
                when no_data_found then
                    --Trackor already deleted
                    v_cnt:= 0;
            end;
        end if;    

        return v_cnt;
    end get_parents_children_count;

    function get_parents_children_count_uid(xitorID number, pcXitorTypeID number, progID number, isParent number, userID number) return number
    as
        cnt number;
        primXitorTypeID number;
        showAllInContainer number;
        rtID number;
        cList varchar2(4000);
        userTtid number;
        userTid number;

        procedure update_xitor_count_user(
            xitorID in number,
            pcXitorTypeID in number,
            userID in number,
            cnt in number)is
            pragma autonomous_transaction;

            e_integrity_constraint exception;
            pragma exception_init(e_integrity_constraint, -02291);
        begin
            merge into xitor_count_user t1
            using
              (select xitorID xitor_id, pcXitorTypeID parent_child_xt_id, userID user_id from dual) t2
            on(t1.xitor_id = t2.xitor_id and t1.parent_child_xt_id = t2.parent_child_xt_id
               and t1.user_id = t2.user_id)
            when matched then
               update set t1.count = cnt
            when not matched then
               insert (xitor_id, parent_child_xt_id, user_id, count)
               values (xitorID, pcXitorTypeID, userID, cnt);

            commit;
        exception
            when dup_val_on_index then
                --another thread inserted data row just before us
                null;
            when e_integrity_constraint then
                --xitor deleted
                null;
        end update_xitor_count_user;
    begin
        userTtid := 0;
        userTid := 0;
        begin
            select count into cnt from xitor_count_user t
            where t.xitor_id=xitorID and t.parent_child_xt_id=pcXitorTypeID
            and t.user_id = userID and t.count is not null;
            return cnt;
        exception
            when no_data_found then
                --count parents and update xitor_count
                null;
        end;

        begin
            for rec in (select x.xitor_id, x.xitor_type_id from xitor x, users u
              where x.xitor_id = u.xitor_id and u.user_id=userID)
            loop
              userTtid := rec.xitor_type_id;
              userTid := rec.xitor_id;
            end loop;

            if isParent = 1 then --get number of parents
               select count(distinct a.xitor_id) into cnt from ancestor_p a
                 where a.child_id = xitorID and a.xitor_type_id = pcXitorTypeID
                  and (
                  /*My Things*/
                  exists (select parent_id from ancestor
                  where child_id = userTid and c_xitor_type_id = userTtid and p_xitor_type_id = pcXitorTypeID and parent_id = a.xitor_id)
                  or exists (select child_id from ancestor
                  where parent_id = userTid and p_xitor_type_id = userTtid and c_xitor_type_id = pcXitorTypeID and child_id = a.xitor_id)
                  or exists (select a.child_id
                  from relation_type rt, relation t, ancestor a
                  where t.child_id = userTid and a.parent_id = t.parent_id and rt.relation_type_id = t.relation_type_id
                  and rt.child_type_id = userTtid and a.p_xitor_type_id = rt.parent_type_id
                  and a.c_xitor_type_id = pcXitorTypeID and a.child_id = a.xitor_id)
                  or exists (select v.key_value from config_field f, config_value_number v
                  where f.config_field_id = v.config_field_id and f.is_my_things_marker = 1
                  and f.xitor_type_id = pcXitorTypeID and f.data_type = 20
                  and v.value_number = userTid and f.obj_xitor_type_id = userTtid and v.key_value = a.xitor_id));

            else --get number of children
               select x.xitor_type_id, rt.show_all_in_trcontainer, rt.relation_type_id
                into primXitorTypeID, showAllInContainer, rtID
               from Xitor x, relation_type rt
                where x.xitor_id=xitorID and rt.parent_type_id=x.xitor_type_id and rt.child_type_id=pcXitorTypeID;

               if showAllInContainer = 1 then
                  select
                  count(distinct t.child_id) into cnt
                  from relation t, xitor c
                  where t.parent_id = xitorID and t.relation_type_id=rtID
                  and t.child_id = c.xitor_id and c.xitor_type_id = pcXitorTypeID
                  and (
                  /*My Things*/
                  exists (select parent_id from ancestor
                  where child_id = userTid and c_xitor_type_id = userTtid and p_xitor_type_id = pcXitorTypeID and parent_id = c.xitor_id)
                  or exists (select child_id from ancestor
                  where parent_id = userTid and p_xitor_type_id = userTtid and c_xitor_type_id = pcXitorTypeID and child_id = c.xitor_id)
                  or exists (select a.child_id
                  from relation_type rt, relation t, ancestor a
                  where t.child_id = userTid and a.parent_id = t.parent_id and rt.relation_type_id = t.relation_type_id
                  and rt.child_type_id = userTtid and a.p_xitor_type_id = rt.parent_type_id
                  and a.c_xitor_type_id = pcXitorTypeID and a.child_id = c.xitor_id)
                  or exists (select v.key_value from config_field f, config_value_number v
                  where f.config_field_id = v.config_field_id and f.is_my_things_marker = 1
                  and f.xitor_type_id = pcXitorTypeID and f.data_type = 20
                  and v.value_number = userTid and f.obj_xitor_type_id = userTtid and v.key_value = c.xitor_id));
               else
                  cList := pkg_relation.get_all_underneath_XTID(primXitorTypeID, progID);

                  select
                  count(distinct t.child_id) into cnt
                  from relation t, xitor c
                  where t.parent_id = xitorID and t.relation_type_id=rtID
                  and t.child_id = c.xitor_id and c.xitor_type_id = pcXitorTypeID
                  and not exists (
                   select r.parent_id from relation r, relation_type rt
                   where r.child_id = t.child_id and rt.cardinality_id = 2
                   and r.relation_type_id = rt.relation_type_id
                   and rt.child_type_id = pcXitorTypeID and rt.parent_type_id <> primXitorTypeID
                   and rt.parent_type_id <> pcXitorTypeID
                   and rt.parent_type_id in (select * from the (select cast(pkg_str.split_str2num(cList) as tableOfNum ) from dual)) )
                  and (
                  /*My Things*/
                  exists (select parent_id from ancestor
                  where child_id = userTid and c_xitor_type_id = userTtid and p_xitor_type_id = pcXitorTypeID and parent_id = c.xitor_id)
                  or exists (select child_id from ancestor
                  where parent_id = userTid and p_xitor_type_id = userTtid and c_xitor_type_id = pcXitorTypeID and child_id = c.xitor_id)
                  or exists (select a.child_id
                  from relation_type rt, relation t, ancestor a
                  where t.child_id = userTid and a.parent_id = t.parent_id and rt.relation_type_id = t.relation_type_id
                  and rt.child_type_id = userTtid and a.p_xitor_type_id = rt.parent_type_id
                  and a.c_xitor_type_id = pcXitorTypeID and a.child_id = c.xitor_id)
                  or exists (select v.key_value from config_field f, config_value_number v
                  where f.config_field_id = v.config_field_id and f.is_my_things_marker = 1
                  and f.xitor_type_id = pcXitorTypeID and f.data_type = 20
                  and v.value_number = userTid and f.obj_xitor_type_id = userTtid and v.key_value = c.xitor_id));
               end if;
            end if;

            update_xitor_count_user(xitorID, pcXitorTypeID, userID, cnt);
        exception
            when no_data_found then
                --Trackor already deleted
                cnt := 0;
        end;

        return cnt;
    end get_parents_children_count_uid;

    function get_all_underneath_xtid(xitorTypeID number, progID number) return varchar2
    as
        ret varchar2(4000);
    begin
        ret := to_char(xitorTypeID);
        for rec in (
            select distinct
                xt.xitor_type_id,
                t.order_number
            from
                relation_type t
                join xitor_type xt on (xt.xitor_type_id = t.child_type_id)
            where xt.program_id = progID
                and xt.is_static_definition = 0
            start with t.parent_type_id = xitorTypeID
            connect by prior t.child_type_id = t.parent_type_id
            order by t.order_number
        ) loop
            if rec.xitor_type_id is not null then
                ret := ret || ',' || to_char(rec.xitor_type_id);
            end if;
        end loop;

        return ret;
    end get_all_underneath_xtid;

    /*getChildrenUnderneathFormID in ASP*/
    function get_children(p_trackor_type_id number,
                          p_program_id number)
    return varchar2
    as
        ret varchar2(4000);
    begin
        ret := '-1';

        for rec in (
            select
                t.child_type_id
            from
                relation_type t
                join xitor_type xt on (xt.xitor_type_id = t.child_type_id)
            where
                xt.is_static_definition = 0
                and xt.program_id = p_program_id
                and t.parent_type_id = p_trackor_type_id
            order by order_number
        ) loop
           if rec.child_type_id is not null then
               ret := ret || ',' || to_char(rec.child_type_id);
           end if;
        end loop;

        return ret;
    end get_children;

    /*getParentsAboveFormID in ASP*/
    function get_parents_above_tt(p_trackor_type_id number)
    return varchar2
    as
        ret varchar2(4000);
    begin
        ret := '-1';

        for rec in (select parent_type_id
                    from relation_type
                    where child_type_id = p_trackor_type_id
                    order by order_number)
        loop
            if rec.parent_type_id is not null then
                ret := ret || ',' || to_char(rec.parent_type_id);
            end if;
        end loop;

        return ret;
    end get_parents_above_tt;

    /*getAllParentsByFromID in ASP*/
    function get_all_parents_above_tt(p_trackor_type_id number,
                                      p_program_id number)
    return varchar2
    as
        ret varchar2(4000);
    begin
        if p_trackor_type_id is not null then
            ret := to_char(p_trackor_type_id);
        else
            ret := '-1';
        end if;

        for rec in (select distinct m.xitor_type_id,
                           t.cardinality_id
                    from relation_type t,
                         (select xt.*
                          from xitor_type xt
                          where xt.is_static_definition = 0
                                and xt.program_id = p_program_id) m
                    where m.xitor_type_id = t.child_type_id
                    start with t.child_type_id = p_trackor_type_id connect by prior t.parent_type_id = t.child_type_id)
        loop
            if rec.xitor_type_id is not null then
                ret := ret || ',' || to_char(rec.xitor_type_id);
            end if;
        end loop;

        return ret;
    end get_all_parents_above_tt;

    procedure get_sub(p_trackor_type_id number,
                      p_program_id number,
                      p_z in out number,
                      p_ret in out varchar2)
    as
    begin
        for rec in (select r.relation_type_id,
                           x.applet_label_id,
                           x.xitor_type_id,
                           x.xitor_type
                     from relation_type r
                         join xitor_type x on (r.child_type_id = x.xitor_type_id)
                     where x.program_id = p_program_id
                           and r.child_type_id = x.xitor_type_id
                           and parent_type_id = p_trackor_type_id
                     order by order_number)
        loop
            if instr(p_ret, ', ' || rec.xitor_type_id || ', ') = 0 then
                p_ret := p_ret || rec.xitor_type_id || ', ' || p_z || ', ';
                p_z := p_z + 1;
            end if;

            get_sub(rec.xitor_type_id, p_program_id, p_z, p_ret);

            p_z := p_z + 1;
        end loop;
    end get_sub;

    /*getOrderByForParentFields in ASP*/
    function get_order_by_for_parent_fields(p_field varchar2,
                                            p_program_id number)
    return varchar2
    as
        ret varchar2(4000);
        z number;
    begin
        ret := 'ORDER BY DECODE(' || p_field || ', -1, -1, ';

        z := 1000010;

        for rec in (select r.relation_type_id,
                           x.applet_label_id,
                           x.xitor_type_id,
                           x.xitor_type
                    from relation_type r
                         join xitor_type x on (r.child_type_id = x.xitor_type_id)
                    where x.program_id = p_program_id
                          and parent_type_id is null
                    order by order_number)
        loop
            if instr(ret, ', ' || rec.xitor_type_id || ', ') = 0 then
                ret := ret || rec.xitor_type_id || ', ' || z || ', ';
                z := z + 1;
            end if;

            get_sub(rec.xitor_type_id, p_program_id, z, ret);

            z := z + 1;
        end loop;

        ret := substr(ret, 1, length(ret) - 2);
        ret := ret || ')';

        return ret;
    end get_order_by_for_parent_fields;

    procedure recalc_blob_ancestor 
    as
        v_cnt number := 0;
    begin
        allowtablemods := true;
        for recrel in (select t.relation_type_id, t.parent_id, x.xitor_type_id, t.child_id, 
                              (select count(*) from ancestor a where a.parent_id = t.child_id) cnt 
                         from relation t, xitor x 
                        where t.is_updated = 1 and t.parent_id = x.xitor_id order by cnt) loop

            for rec in (select blob_id from blob_ancestor ba where ba.blob_owner_id = recrel.child_id) loop
                begin
                    pkg_dl_support.update_blob_ancestor(rec.blob_id, recrel.parent_id);
                exception
                    when others then null;
                end;

                for recparents in (select parent_id from ancestor a where a.child_id = recrel.parent_id) loop
                    begin
                        pkg_dl_support.update_blob_ancestor(rec.blob_id, recParents.parent_id);
                    exception
                        when others then null;
                    end;
                end loop;
            end loop;

            --Reset isUpdated flag          
            update relation t set t.is_updated = 0 
             where t.relation_type_id = recrel.relation_type_id and t.parent_id = recrel.parent_id and t.child_id = recrel.child_id;

            v_cnt := v_cnt + 1;

            if mod(v_cnt, 10000) = 0 then
                commit;
            end if;     

        end loop;

        commit;
        allowtablemods := false;
    end;

    function paths_wout_xid(
        p_parent_id in relation.parent_id%type,
        p_child_id in relation.child_id%type,
        p_xid_in_path in xitor.xitor_id%type)
        return number
    is
        v_paths_wout_xid_cnt number;
    begin
        select count(*) into v_paths_wout_xid_cnt from (
            select sys_connect_by_path(child_id, ',')  || ',' p
            from relation where parent_id = p_parent_id
            start with child_id = p_child_id connect by prior parent_id = child_id) p
        where p.p not like '%,' || p_xid_in_path || ',%';

        return v_paths_wout_xid_cnt;
    end paths_wout_xid;

    procedure recreate_ancestor_type(p_pid in program.program_id%type) is
    begin
        allowtablemods := true;
        delete from ancestor_type where program_id = p_pid;

        allowtablemods := true;
        for rec in (
                select distinct t.parent_type_id from relation_type t
                where t.parent_type_id is not null and t.program_id = p_pid) loop
            insert into ancestor_type(child_type_id, parent_type_id, relation_type_id, program_id)
                select distinct m.xitor_type_id, rec.parent_type_id, t.relation_type_id, p_pid
                    from relation_type t,
                         xitor_type m
                    where m.xitor_type_id = t.child_type_id
                    start with t.parent_type_id = rec.parent_type_id
                    connect by prior t.child_type_id = t.parent_type_id;
        end loop;
        allowtablemods := false;
    end recreate_ancestor_type;

    function get_parents_up_to_many_many(
        p_child_ttid in xitor_type.xitor_type_id%type,
        p_pid in program.program_id%type
    ) return tableofnum is
        v_ttids tableofnum;
    begin
        begin
            select distinct
                xt.xitor_type_id
            bulk collect into v_ttids
            from
                relation_type t
                join xitor_type xt on (xt.xitor_type_id = t.child_type_id)
            where xt.is_static_definition = 0
                and xt.program_id = p_pid
            start with t.child_type_id = p_child_ttid
            connect by prior t.parent_type_id = t.child_type_id and prior t.cardinality_id <> 3;
        exception
            when no_data_found then
                v_ttids := tableofnum();
        end;
        return v_ttids;
    end get_parents_up_to_many_many;

    procedure unassign_fields(p_prim_ttid number, p_parent_ttid number, p_unassign_tid number default 1) as
        v_rtid relation_type.relation_type_id%type;
    begin
        if p_parent_ttid is not null then
            select rtid into v_rtid from (
            select relation_type_id as rtid
            from relation_type
            where parent_type_id = p_parent_ttid
                and child_type_id = p_prim_ttid
            union select null from dual) where rownum < 2;
        else
            select rtid into v_rtid from (
            select relation_type_id as rtid
            from relation_type
            where parent_type_id is null
                and child_type_id = p_prim_ttid
            union select null from dual) where rownum < 2;
        end if;

        delete from config_group_elem_xref
        where config_group_elem_xref_id in(
            select cgfx.config_group_elem_xref_id
            from
                config_group cg
                join config_group_elem_xref cgfx on (cgfx.config_group_id = cg.config_group_id)
                join config_element e on (cgfx.config_element_id = e.config_element_id)
                join config_field c on (c.config_field_id = e.config_field_id)
            where cg.xitor_type_id = p_prim_ttid
               and c.xitor_type_id = p_parent_ttid
               and (
                    ((c.config_field_name = 'XITOR_KEY') and p_unassign_tid = 1)
                    or (c.config_field_name <> 'XITOR_KEY')
                )
        );

    end unassign_fields;

end pkg_relation;
/