from __future__ import annotations

import time, json, pendulum

import teradatasql


from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# initiate DAG Context
with DAG(
    dag_id='VantageCloud_Lake_Demo_Setup',
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['demo'],
) as dag:
    
    @task(task_id='Create_SYSBDA')
    def create_sys_dba(**kwargs):
        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        
        # 1. connect as DBC and create sysdba and grant privileges

        dbc_name = session_hierarchy['DBC']['username']
        dbc_pwd = session_hierarchy['DBC']['password']
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']


        with teradatasql.connect(host = session_env['host'], 
                             user = dbc_name, 
                             password = dbc_pwd) as con:
            cur = con.cursor()

            qry = f'''
                CREATE USER {name} FROM DBC
                  AS PERM  = 8E11          /* 500GB */
                    ,SPOOL = 8E11          /* 500GB */
                    ,PASSWORD = "{pwd}"
                    ,ACCOUNT = ('$M')
                    ,TIME ZONE = '-4:00'   /* US Eastern Daylight Time */
                    ,DEFAULT ROLE = ALL  ;
                '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5612") >= 1:
                    pass
                else:
                    raise


            queries = [f'''--Grant All privileges on SYSDBA to itself.
                        GRANT all on "{name}" to "{name}" WITH GRANT OPTION;''', 
                      f'''--Grant system-level privileges to SYSDBA.
                        GRANT MONRESOURCE, MONSESSION, ABORTSESSION, SETSESSRATE, SETRESRATE, REPLCONTROL, 
                        CREATE PROFILE, CREATE ROLE, DROP PROFILE, DROP ROLE 
                        to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT EXECUTE, SELECT, STATISTICS, SHOW on DBC to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT UDTTYPE, UDTMETHOD on SYSUDTLIB to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT SELECT, INSERT, UPDATE, DELETE on "Sys_Calendar" to "{name}" WITH GRANT OPTION;''',
                      f'''GRANT SELECT, EXECUTE FUNCTION, EXECUTE PROCEDURE on TD_SYSFNLIB to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT SELECT, EXECUTE FUNCTION, EXECUTE PROCEDURE on TD_MLDB to "{name}" WITH GRANT OPTION;''',
                      f'''GRANT SELECT, EXECUTE FUNCTION, EXECUTE PROCEDURE on syslib to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT EXECUTE FUNCTION on TD_SYSFNLIB.READ_NOS to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT CREATE DATASET SCHEMA on SYSUDTLIB to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT SELECT on TD_METRIC_SVC to "{name}" WITH GRANT OPTION;''', 
                      f'''GRANT ALL ON retail_sample_data to "{name}" WITH GRANT OPTION;''', 
                      f'''/* Grant SYSDBA the privileges to grant others to System Roles, except TD_COMPUTE_CLUSTER_ADMIN. */
                        /* This is to lockdown who may administer Compute Groups.                                        */ 
                        GRANT  TD_USER_ADMIN
                          ,TD_DATA_SCIENTIST
                          ,TD_FLOW_COMPOSER
                          ,TD_CURRENT_QUERIES_ADMIN
                          ,TD_CURRENT_QUERIES_VIEW
                          ,TD_QUERY_HISTORY_VIEW 
                        TO {name} WITH ADMIN OPTION;''', 
                      f'''GRANT LOGON ON ALL TO {name} WITH NULL PASSWORD;''']
            for qry in queries:
                cur.execute(qry)
        return 'SYUSDBA User Created'

    create_sysdba = create_sys_dba()

    @task(task_id="create_user_hierarchy")
    def create_user_hierarchy(**kwargs):
        #/* -------------------------------------------------------- */
        #-- Perform this as SYSDBA.
        #-- Create User hierachy
        #/* -------------------------------------------------------- */
        
        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            qry = f'''
                CREATE USER "Users" FROM SYSDBA
                  AS PERM  = 3E10       /*   30GB */
                    ,SPOOL = 1E11      /* 100GB */
                    ,PASSWORD = "{pwd}"
                    ,ACCOUNT = ('$M')
                    ,TIME ZONE='-4:00'
                    ,DEFAULT ROLE=ALL;
                    '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5612") >= 1:
                    pass
                else:
                    raise

            qry = f'''
            CREATE USER "BusinessUsers" FROM Users
              AS PERM  = 2E10       /*   20GB */
                ,SPOOL = 1E11      /* 100GB */
                ,PASSWORD = "{pwd}"
                ,ACCOUNT = ('$M')
                ,TIME ZONE = '-4:00'
                ,DEFAULT ROLE = ALL ;
            '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5612") >= 1:
                    pass
                else:
                    raise

            qry = f'''
            CREATE USER "SupportUsers" FROM Users
              AS PERM =  1E10       /*   10GB */
                ,SPOOL = 1E11      /* 100GB */
                ,PASSWORD = "{pwd}"
                ,ACCOUNT = ('$M')
                ,TIME ZONE = '-4:00'
                ,DEFAULT ROLE = ALL;
                '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5612") >= 1:
                    pass
                else:
                    raise

            # create support users from array of support_users
            for u in session_hierarchy['users']['support_users']:
                qry = f'''
                /* Create Compute Group administrator */
                CREATE USER {u['username']} FROM SupportUsers
                  AS PERM = 0       
                    ,SPOOL = 1E10       /* 10GB */
                    ,ACCOUNT = ('$M')
                    ,DEFAULT ROLE = ALL
                    ,PASSWORD = "{u['password']}" ;
                '''
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise
            
        return 'User Hierarchy Created'
    

    create_user_hierarchy = create_user_hierarchy()
    
    @task(task_id="grant_compute_group_admin")
    def grant_compute_group_admin(**kwargs):
        #/* -------------------------------------------------------- */
        #-- Perform this as *** DBC ***.
        #-- Grant privileges to Compute Group administrator. 
        #-- NB: To lockdown who may create Compute Clusters, there is
        #--     intentionally only one CG admin.
        #/* -------------------------------------------------------- */
        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        
        dbc_name = session_hierarchy['DBC']['username']
        dbc_pwd = session_hierarchy['DBC']['password']


        with teradatasql.connect(host = session_env['host'], 
                             user = dbc_name, 
                             password = dbc_pwd) as con:
            cur = con.cursor()

            # iterate over all support users
            for u in session_hierarchy['users']['support_users']:

                queries = [f'''-- Compute Group super user ONLY.
                            GRANT TD_USER_ADMIN, TD_COMPUTE_CLUSTER_ADMIN, TD_FLOW_COMPOSER, TD_CURRENT_QUERIES_ADMIN TO {u['username']};''', 
                          f'''GRANT SELECT ON DBC TO {u['username']};''', 
                          f'''GRANT DROP COMPUTE PROFILE to "{u['username']}" WITH GRANT OPTION;''',
                          f'''GRANT CREATE COMPUTE PROFILE to "{u['username']}" WITH GRANT OPTION;''', 
                          f'''grant CREATE COMPUTE GROUP to "{u['username']}" WITH GRANT OPTION;''',
                          f'''grant DROP COMPUTE GROUP to "{u['username']}" WITH GRANT OPTION;''', 
                          f'''GRANT LOGON ON ALL TO {u['username']} WITH NULL PASSWORD;''']
                for qry in queries:
                    cur.execute(qry)
        
        return 'Compute Group Admin Created'
    
    grant_compute_group_admin = grant_compute_group_admin()
    
    @task(task_id="create_environment_hierarchy")
    def create_environment_hierarchy(**kwargs):
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Create Environment database hierachy
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            # Create root database
            qry = f'''
            CREATE DATABASE {session_hierarchy['repositories']['name']} from {name} 
              AS PERM = {session_hierarchy['repositories']['perm']} ;    /* 300GB */
              '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5612") >= 1:
                    pass
                else:
                    raise
                    
            # Create root Public Auth object with NULL username and password
            qry = f'''REPLACE AUTHORIZATION {session_hierarchy['repositories']['name']}.PubAuth
                    user '' password '';'''
            cur.execute(qry)

            # Create child databases
            for d in session_hierarchy['repositories']['databases']:
                qry = f'''
                CREATE DATABASE {d['name']} from Repositories  AS
                  DEFAULT STORAGE = {d['default_storage']} OVERRIDE ON ERROR,
                  PERM = {d['perm']} ;     /* 200GB */
                '''
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise

                qry = f'''GRANT Select on DBC to {d['name']} with grant option;'''
                cur.execute(qry)

                qry = f'''GRANT Select on Sys_Calendar to {d['name']} with grant option;'''
                cur.execute(qry)

                qry = f'''GRANT Select on TD_METRIC_SVC to {d['name']} with grant option;'''
                cur.execute(qry)

                qry = f'''GRANT EXECUTE on retail_sample_data.demo_Auth_NOS to "{d['name']}" WITH GRANT OPTION;'''
                cur.execute(qry)
                
                # Grant execute to the PubAuth object
                qry = f'''GRANT EXECUTE on {session_hierarchy['repositories']['name']}.PubAuth to "{d['name']}" WITH GRANT OPTION;'''
                cur.execute(qry)
                
        return 'Environment Hierarchy Created'
    
    create_environment_hierarchy = create_environment_hierarchy()
    
    @task(task_id="create_profiles")
    def create_profiles(**kwargs):
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Create Profiles
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            qry = f'''
                CREATE PROFILE "P_BusGrpA" AS
                SPOOL=1E11      /* 100GB */
                TEMPORARY=1E8   /* 100MB */
                ACCOUNT=NULL
                DEFAULT DATABASE=NULL ;
                '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5652") >= 1:
                    pass
                else:
                    raise

            qry = f'''
                CREATE PROFILE "P_SupportUsers" AS
                SPOOL=1E11      /* 100GB */
                TEMPORARY=1E8   /* 100MB */
                ACCOUNT=NULL
                DEFAULT DATABASE=NULL ;
                '''
            try:
                cur.execute(qry)
            except Exception as e:
                # check if user exists
                if str(e.args).find("Error 5652") >= 1:
                    pass
                else:
                    raise

        return 'User Profiles Created'
    
    create_profiles = create_profiles()
    
    @task(task_id='create_roles')
    def create_roles(**kwargs):
        
          
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Create Roles for Compute Groups admin, General TD access, NOS, etc.
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            for d in session_hierarchy['compute_group_roles']:

                qry = f'''create role {d['name']};'''
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise

            queries = ['''
            -- Create a Teradata general admin role 
            create role R_TD_Admin; ''', 
                       '''
            -- Create Roles for general TD access rights 
            create role R_TD_General;''', 
                      '''
            -- Create Roles for NOS access rights 
            create role R_NOS;''', 
                      '''
            -- Create ANALYTICS Role
            create role R_Analytics_1;''', 
                      '''
            -- Create Demo Role (Read only)
            create role R_Demo_R;''', 
                      '''
            -- Create Demo Role (Read/Write)
            create role R_Demo_RW;''']

            for qry in queries:
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise
                        
        return 'Roles Created'
    
    create_roles = create_roles()
    
    @task(task_id='grant_role_privs')
    def grant_role_privs(**kwargs):
        
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Grant privileges to Roles.
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            queries = ['''/* R_TD_Admin */
                        GRANT EXECUTE, SELECT, STATISTICS, SHOW ON "DBC" to R_TD_Admin;''', 
                      '''GRANT  TD_CURRENT_QUERIES_ADMIN to R_TD_Admin;''', 
                      '''/* R_TD_General */
                        GRANT SELECT ON TD_METRIC_SVC to R_TD_General;''', 
                      '''GRANT TD_CURRENT_QUERIES_VIEW to R_TD_General;''', 
                      '''GRANT TD_QUERY_HISTORY_VIEW   to R_TD_General;''', 
                      '''GRANT EXECUTE FUNCTION ON TD_SYSFNLIB to R_TD_General;''',
                      '''GRANT EXECUTE FUNCTION ON TD_MLDB to R_TD_General;''',
                      '''GRANT SELECT ON DBC to R_TD_General;''', 
                      '''GRANT SELECT, EXECUTE FUNCTION, EXECUTE PROCEDURE ON syslib to R_TD_General;''', 
                      '''/* R_NOS */
                        GRANT EXECUTE FUNCTION ON TD_SYSFNLIB.READ_NOS  to R_NOS;''', 
                      '''GRANT EXECUTE FUNCTION ON TD_SYSFNLIB.WRITE_NOS to R_NOS;''', 
                      '''GRANT CREATE DATASET SCHEMA on SYSUDTLIB to R_NOS;''', 
                      '''/* R_Analytics_1 */
                        GRANT TD_DATA_SCIENTIST TO R_Analytics_1;''',
                      '''/* R_Demo_R */
                        GRANT Select  on demo to R_Demo_R ; ''', 
                      '''/* R_Demo_R */
                        GRANT Select  on demo_ofs to R_Demo_R ;''',
                      '''/* R_Demo_RW */
                        GRANT Create Table, Drop Table, Insert, Update, Delete, Select,
                        Create AUTHORIZATION on demo to R_Demo_RW ;''',
                      '''/* R_Demo_RW */
                        GRANT Create Table, Drop Table, Insert, Update, Delete, Select,
                        Create AUTHORIZATION on demo_ofs to R_Demo_RW ;''',
                      '''GRANT Create View, Drop View, Select on demo to R_Demo_RW;''',
                      '''GRANT Create View, Drop View, Select on demo_ofs to R_Demo_RW;''']

            for qry in queries:
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise
        return 'All Roles Created'
    
    grant_role_privs = grant_role_privs()
    
    @task(task_id = 'create_comp_grp_prf')
    def create_comp_grp_prf(**kwargs):
        # /* -------------------------------------------------------- */
        # -- Perform this as cgadmin
        # -- Create Compute Groups / Compute Profiles
        # /* -------------------------------------------------------- */

        # /* Learnings: 
        # --  1) Some of these statements do not run under ODBC or .NET (only JDBC) 
        # --  2) If you create a Compute Profile "INITIALLY_SUSPENDED", it goes through its
        # --     states of "Deploying", "Hibernating", "Hibernated". This takes about 10 mins.
        # --     Once it's in the "Hibernated" (suspend) state, currently we are not able to
        # --     "Resume" it. Therefore, start/stop the CG by Create/Drop the Compute Profile.
        # */
        
        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['users']['support_users'][0]['username']
        pwd = session_hierarchy['users']['support_users'][0]['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            # iterate through the groups and profiles
            for cg in session_hierarchy['compute_groups']:

                qry = f'''CREATE COMPUTE GROUP {cg['name']} USING QUERY_STRATEGY('{cg['type']}');'''
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 4822") >= 1:
                        pass
                    else:
                        raise

                # iterate over each child profile in the group
                for cp in cg['profiles']:

                    qry = f'''
                    /* This profile is configured to be available from 9am to 9pm EDT. */
                    CREATE COMPUTE PROFILE {cp['name']} IN COMPUTE GROUP {cg['name']}
                    ,INSTANCE = {cp['size']}
                    ,INSTANCE TYPE = {cp['type']}   
                     USING
                         MIN_COMPUTE_COUNT ({cp['min']})
                         MAX_COMPUTE_COUNT ({cp['max']})
                         SCALING_POLICY ('STANDARD')
                        -- INITIALLY_SUSPENDED ('true')
                         START_TIME ('0 13 * * MON-FRI')
                         END_TIME ('0 01 * * MON-SAT')
                         COOLDOWN_PERIOD (30) ;
                    '''
                    try:
                        cur.execute(qry)
                    except Exception as e:
                        # check if user exists
                        if str(e.args).find("Error 4826") >= 1:
                            pass
                        else:
                            raise
                            
        return 'Compute Groups and Profiles Created'
    
    create_comp_grp_prf = create_comp_grp_prf()
    
    @task(task_id = 'create_users')
    def create_users(**kwargs):
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Create end-users
        # -- Recommendation: Create new users under "BusinessUsers"
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            for u in session_hierarchy['users']['business_users']:

                qry = f'''
                CREATE USER {u['username']} FROM BusinessUsers
                 AS PERM =  1E10 
                   ,Spool = 0    /* Spool is defined in the Profile */
                   ,Profile = "P_BusGrpA"
                   ,PASSWORD = "{u['password']}"
                   ,ACCOUNT = ('$M')
                   ,TIME ZONE = '-4:00'
                   ,DEFAULT ROLE = ALL
                   ,COMPUTE GROUP = {u['compute_group']};
                '''
                try:
                    cur.execute(qry)
                except Exception as e:
                    # check if user exists
                    if str(e.args).find("Error 5612") >= 1:
                        pass
                    else:
                        raise

                qry = f'''GRANT LOGON ON ALL TO {u['username']} WITH NULL PASSWORD;'''
                cur.execute(qry)
                
                qry = f'''GRANT TD_DATA_SCIENTIST TO {u['username']};'''
                cur.execute(qry)

        return 'End Users Created'
    
    create_users = create_users()
    
    @task(task_id = 'grant_compute')
    def grant_compute(**kwargs):
        
        # /* -------------------------------------------------------- */
        # -- Perform this as CGADMIN.
        # -- Grant COMPUTE GROUP to every user
        # /* -------------------------------------------------------- */

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['users']['support_users'][0]['username']
        pwd = session_hierarchy['users']['support_users'][0]['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            for u in session_hierarchy['users']['business_users']:

                qry = f'''GRANT COMPUTE GROUP {u['compute_group']} TO {u['username']}'''
                cur.execute(qry)

        return 'Compute Group Permissions Granted'
    
    grant_compute = grant_compute()
    
    @task(task_id = 'grant_role_to_users')
    def grant_role_to_users(**kwargs):
        
        # /* -------------------------------------------------------- */
        # -- Perform this as SYSDBA.
        # -- Grant Roles to User (including access to Compute Groups)
        # /* -------------------------------------------------------- */

        # --##############################################################################
        # -- Refer to the environment setup file (VCLE_environment_setup_vXX) for 
        # -- details on privileges assigned to each Role. 
        # -- This is an overview.
        # --
        # -- ROLENAME                RIGHTS
        # --                         
        # -- R_TD_Admin              Access to DBC and TD_CURRENT_QUERIES_ADMIN
        # -- R_TD_General            Access to MSS, TD_SYSFNLIB, SYSLIB, Select on DBC
        # -- R_NOS                   Access to TD_SYSFNLIB.READ_NOS, CREATE DATASET SCHEMA on SYSUDTLIB
        # -- R_Analytics_1           TD_DATA_SCIENTIST
        # -- R_Demo_R              Read (only) on Demo databases
        # -- R_Demo_RW             Read/Write  on Demo databases	

        # -- CR_BusGrpA_STD  Non-admin privileges to use Standard Compute Group CG_BusGrpA_STD
        # -- CR_BusGrpA_ANL  Non-admin privileges to use Analytic Compute Group CG_BusGrpA_ANL
        # --##############################################################################

        session_env = Variable.get('environment', deserialize_json=True)
        session_hierarchy = Variable.get('hierarchy', deserialize_json=True)
        name = session_hierarchy['SYSDBA']['username']
        pwd = session_hierarchy['SYSDBA']['password']

        with teradatasql.connect(host = session_env['host'], 
                             user = name, 
                             password = pwd) as con:
            cur = con.cursor()

            for u in session_hierarchy['users']['business_users']:

                queries = [f'''-- Grant Non-Admin user to the Compute Group (CG) Roles
                            -- NB: User cannot use the CG until it is "SET" as their default CG. 
                            GRANT {u['role']} to {u['username']};''',
                          f'''-- Grant other Roles to user
                            GRANT  R_TD_General   to {u['username']} ;''', 
                          f'''GRANT  R_Analytics_1 to {u['username']};''', 
                          f'''GRANT R_NOS to {u['username']};''', 
                          f'''GRANT R_Demo_RW to {u['username']} ;''', 
                          f'''--special grant to get access to the NOS AUTH in retail_sample_data
                            GRANT ALL ON retail_sample_data TO {u['username']} ;''', 
                          f'''grant execute on retail_sample_data.demo_Auth_NOS to "{u['username']}" WITH GRANT OPTION;''',
                          f'''--special grant to get access to the root PubAuth
                           GRANT EXECUTE on {session_hierarchy['repositories']['name']}.PubAuth to "{u['username']}" WITH GRANT OPTION;''',
                          f'''-- Set default Compute Group to existing user.
                            -- NB: This may also be done by the user via "SET SESSION COMPUTE GROUP <CG_Name>;"
                            MODIFY user {u['username']} as COMPUTE GROUP = {u['compute_group']} ;''']

                for qry in queries:
                    cur.execute(qry)

        return 'Roles Granted to Users'
    
    grant_role_to_users = grant_role_to_users()
        
    create_sysdba >> create_user_hierarchy >> [grant_compute_group_admin, create_environment_hierarchy, create_profiles, create_roles] >> grant_role_privs >> create_comp_grp_prf >> create_users >> [grant_compute, grant_role_to_users]
