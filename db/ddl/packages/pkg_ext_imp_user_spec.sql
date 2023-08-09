CREATE OR REPLACE PACKAGE PKG_EXT_IMP_USER 
/*
 * Copyright 2003-2023 OneVizion, Inc. All rights reserved.
 */
as
    c_password_min_length constant number := 6;

   /**
    *   Create/update OneVizion user accounts, user Trackor records, and user settings
    * <p />
    *   Expected header:
    * <br />
    *   UN,EMAIL,IS_DISABLED,MUST_CHANGE_PASSWORD,INIT_PASSWORD_RESET,IS_SUPERUSER,
    *   APPLICATION(or Menu),IS_EXTERNAL_LOGIN,IS_ADD_QUOTE_DELIM,
    *   CHECKBOX_MODE(or CHECKBOX_MODE_ID),IS_AUTO_SAVE_TB_GRID_CHANGES,
    *   COORDINATE_MODE(or COORDINATE_MODE_ID),DATE_FORMAT,DEFAULT_PAGE(or DEFAULT_PAGE_ID),
    *   IS_EXACT_SEARCH_CLIPBOARD,FV_LIST_MODE(or FV_LIST_MODE_ID),
    *   IS_COMMENTS_ON_MOUSE_OVER,MFA_TYPE,IS_SHOW_USER_CHAT_STATE_IN_GRID
    * <br />
    *   GRID_EDIT_MODE(or GRID_EDIT_MODE_ID),
    *   IS_HIDE_START_DATE,IS_MUTE_NEW_EVENTS,
    *   APP_LANG_NAME(or APP_LANG_ID),
    *   IS_SHOW_TIP_OF_THE_DAY,THOUSANDS_SEPARATOR,
    *   TIME_FORMAT,OWNER_XITOR_TYPE,
    *   MAXIMIZE_NEW_WINDOW,LINKED_VALUES_DISP_MODE,
    *   IS_CASE_SENSITIVE_SORTING,IS_HIDE_FIELD_TAB_PREFIX,IS_MAIN_MENU_STICKY,PHONE_NUMBER,
    *   Security Role 1,Security Role 2,Security Role 3, SECURITY_ROLES,
    *   Global Views,Global Filters,
    *   Discipline 1,Discipline 2,Discipline 3
    * <p />
    *   Note: UN - is a name of a user;
    * <br />
    *   Security Role - is a name of a security role;
    * <br />
    *   APPLICATION (or Menu) - is a name of an application menu;
    * <br />
    *   Global Views - is a boolean parameter to assign global views to a user, it has to be as '0' or '1';
    * <br />
    *   Global Filters - is a boolean parameter to assign global filters to a user, it has to be as '0' or '1';
    * <br />
    *   Discipline - is a name of a discipline.
    * <br />
    *   OWNER_XITOR_TYPE - is a name of owner trackor type for user.
    * <br />
    *   SECURITY_ROLES - comma-separated list of Security Roles.
    * <br />
    *   PHONE_NUMBER - is a string in the format "+XXX-XXX-XXXXXXX" (for example, "+1-123-1234567").
    * <br />
    *   Supports import of Config Field values to the User Trackor
    * <br />
    *   Supports relations creation between User and parent Trackor Records
    * <br />
    *   You have to use Trackor Type Name as Excel column name to create relation between this trackor type and "USER" trackor type
    * <p />
    *   To create template file for import you can use the export data from "Administer account" page and "User" trackor type page
    *
    *   @param p_rid           - Import run id
    *   @param p_update_xitors - Load or not fields, which presente in User trackor type
    *   @param p_user_xt_id    - User Trackor Type id
    *   @param p_key_col_name  - Key field name for User trackor Type
    */
    procedure usersload(p_rid in imp_run.imp_run_id%type,
                        p_update_xitors in number default 0,
                        p_user_ttid in xitor_type.xitor_type_id%type default null,
                        p_key_col_name in varchar2 default null);

end pkg_ext_imp_user;
/